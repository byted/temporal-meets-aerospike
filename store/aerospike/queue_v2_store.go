package aerospike

import (
	"context"
	"encoding/json"
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
	enumspb "go.temporal.io/api/enums/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
)

// QueueV2 backs Temporal's DLQs. Each queue is one metadata record holding the
// next message id, plus bucketed message records -- the same shape as history
// tasks, since the access pattern is the same: append at the head, read a range
// in order, delete a prefix.
const (
	setQueueV2Meta = "q2meta"
	setQueueV2Msg  = "q2msg"

	binNextMessageID = "next_id"
	binQueueType     = "q_type"
	binQueueName     = "q_name"

	// queueBucketShift buckets messages at 4096 ids each.
	queueBucketShift = 12
)

func (k *keyBuilder) queueMetaKey(queueType p.QueueV2Type, name string) (*as.Key, error) {
	return k.newKey(setQueueV2Meta, fmt.Sprintf("%d:%s", queueType, name))
}

func (k *keyBuilder) queueMessageKey(queueType p.QueueV2Type, name string, bucket int64) (*as.Key, error) {
	return k.newKey(setQueueV2Msg, fmt.Sprintf("%d:%s:%d", queueType, name, bucket))
}

func queueBucket(messageID int64) int64 { return messageID >> queueBucketShift }

type queueV2Store struct{ client *client }

var _ p.QueueV2 = (*queueV2Store)(nil)

func newQueueV2Store(c *client) *queueV2Store { return &queueV2Store{client: c} }

func (s *queueV2Store) begin() *txnScope {
	txn := as.NewTxnWithCapacity(16, 16)
	read, write, del := s.client.txnPolicies(txn)
	return &txnScope{client: s.client, txn: txn, read: read, write: write, del: del}
}

func (s *queueV2Store) CreateQueue(
	ctx context.Context,
	request *p.InternalCreateQueueRequest,
) (*p.InternalCreateQueueResponse, error) {
	key, err := s.client.keys.queueMetaKey(request.QueueType, request.QueueName)
	if err != nil {
		return nil, err
	}

	// Message ids start at 1: zero doubles as "nothing enqueued yet".
	err = s.client.as.PutBins(withCtxW(ctx, s.client.create), key,
		as.NewBin(binNextMessageID, int64(p.FirstQueueMessageID)),
		as.NewBin(binQueueType, int(request.QueueType)),
		as.NewBin(binQueueName, request.QueueName),
	)
	if err != nil {
		if isKeyExists(err) {
			// The suite asserts the message names both the type and the queue,
			// so wrap rather than return the bare sentinel.
			return nil, fmt.Errorf("%w: type %d, name %q",
				p.ErrQueueAlreadyExists, int(request.QueueType), request.QueueName)
		}
		return nil, convertError("CreateQueue", err)
	}
	return &p.InternalCreateQueueResponse{}, nil
}

func (s *queueV2Store) EnqueueMessage(
	ctx context.Context,
	request *p.InternalEnqueueMessageRequest,
) (*p.InternalEnqueueMessageResponse, error) {
	metaKey, err := s.client.keys.queueMetaKey(request.QueueType, request.QueueName)
	if err != nil {
		return nil, err
	}

	// Claim an id and write the message atomically: a gap would be harmless,
	// but a reused id would silently overwrite a message.
	t := s.begin()
	defer t.finish()

	rec, err := s.client.as.Get(withCtx(ctx, t.read), metaKey, binNextMessageID)
	if err != nil {
		if isNotFound(err) {
			return nil, p.NewQueueNotFoundError(request.QueueType, request.QueueName)
		}
		return nil, convertError("EnqueueMessage", err)
	}
	messageID := binInt64(rec, binNextMessageID)

	msgKey, err := s.client.keys.queueMessageKey(request.QueueType, request.QueueName, queueBucket(messageID))
	if err != nil {
		return nil, err
	}
	if _, err := s.client.as.Operate(withCtxW(ctx, t.write), msgKey,
		as.MapPutOp(kOrderedMap, binTaskMap, messageID, blobValue(request.Blob))); err != nil {
		return nil, convertError("EnqueueMessage", err)
	}
	if err := s.client.as.PutBins(withCtxW(ctx, t.write), metaKey,
		as.NewBin(binNextMessageID, messageID+1)); err != nil {
		return nil, convertError("EnqueueMessage", err)
	}

	if err := t.commit("EnqueueMessage"); err != nil {
		return nil, err
	}
	return &p.InternalEnqueueMessageResponse{
		Metadata: p.MessageMetadata{ID: messageID},
	}, nil
}

func (s *queueV2Store) ReadMessages(
	ctx context.Context,
	request *p.InternalReadMessagesRequest,
) (*p.InternalReadMessagesResponse, error) {
	metaKey, err := s.client.keys.queueMetaKey(request.QueueType, request.QueueName)
	if err != nil {
		return nil, err
	}
	rec, err := s.client.as.Get(withCtx(ctx, s.client.read), metaKey, binNextMessageID)
	if err != nil {
		if isNotFound(err) {
			return nil, p.NewQueueNotFoundError(request.QueueType, request.QueueName)
		}
		return nil, convertError("ReadMessages", err)
	}
	nextID := binInt64(rec, binNextMessageID)

	minID := int64(p.FirstQueueMessageID)
	if len(request.NextPageToken) > 0 {
		var tok queuePageToken
		if err := json.Unmarshal(request.NextPageToken, &tok); err != nil {
			return nil, p.ErrInvalidReadQueueMessagesNextPageToken
		}
		minID = tok.LastID + 1
	}

	if request.PageSize <= 0 {
		return nil, p.ErrNonPositiveReadQueueMessagesPageSize
	}
	pageSize := request.PageSize

	resp := &p.InternalReadMessagesResponse{}
	lastID := int64(0)

	for bucket := queueBucket(minID); bucket <= queueBucket(nextID); bucket++ {
		key, err := s.client.keys.queueMessageKey(request.QueueType, request.QueueName, bucket)
		if err != nil {
			return nil, err
		}
		bucketRec, err := s.client.as.Operate(withCtxW(ctx, s.client.write), key,
			as.MapGetByKeyRangeOp(binTaskMap, minID, nextID, as.MapReturnType.KEY_VALUE))
		if err != nil {
			if isNotFound(err) {
				continue
			}
			return nil, convertError("ReadMessages", err)
		}
		for _, entry := range recordMapPairs(bucketRec, binTaskMap) {
			id, ok := asInt64(entry.Key)
			if !ok || id < minID {
				continue
			}
			if len(resp.Messages) == pageSize {
				resp.NextPageToken, _ = json.Marshal(queuePageToken{LastID: lastID})
				return resp, nil
			}
			blob := blobFromValue(entry.Value)
			if blob == nil {
				continue
			}
			// Validate the *stored* encoding string, not the decoded blob:
			// NewDataBlob silently maps an unrecognised encoding to
			// ENCODING_TYPE_UNSPECIFIED, which is itself a valid enum value, so
			// checking the decoded field would never catch corruption.
			if err := validateStoredEncoding(entry.Value); err != nil {
				return nil, err
			}
			resp.Messages = append(resp.Messages, p.QueueV2Message{
				MetaData: p.MessageMetadata{ID: id},
				Data:     blob,
			})
			lastID = id
		}
	}

	if len(resp.Messages) > 0 {
		resp.NextPageToken, _ = json.Marshal(queuePageToken{LastID: lastID})
	}
	return resp, nil
}

type queuePageToken struct {
	LastID int64 `json:"i"`
}

func (s *queueV2Store) RangeDeleteMessages(
	ctx context.Context,
	request *p.InternalRangeDeleteMessagesRequest,
) (*p.InternalRangeDeleteMessagesResponse, error) {
	metaKey, err := s.client.keys.queueMetaKey(request.QueueType, request.QueueName)
	if err != nil {
		return nil, err
	}
	if _, err := s.client.as.Get(withCtx(ctx, s.client.read), metaKey, binNextMessageID); err != nil {
		if isNotFound(err) {
			return nil, p.NewQueueNotFoundError(request.QueueType, request.QueueName)
		}
		return nil, convertError("RangeDeleteMessages", err)
	}

	maxID := request.InclusiveMaxMessageMetadata.ID
	if maxID < p.FirstQueueMessageID {
		return nil, fmt.Errorf("%w: %d is below the first queue message id %d",
			p.ErrInvalidQueueRangeDeleteMaxMessageID, maxID, p.FirstQueueMessageID)
	}

	deleted := int64(0)

	for bucket := int64(0); bucket <= queueBucket(maxID); bucket++ {
		key, err := s.client.keys.queueMessageKey(request.QueueType, request.QueueName, bucket)
		if err != nil {
			return nil, err
		}
		// Inclusive upper bound, so the range end is maxID+1.
		rec, err := s.client.as.Operate(withCtxW(ctx, s.client.write), key,
			as.MapRemoveByKeyRangeOp(binTaskMap, int64(0), maxID+1, as.MapReturnType.COUNT))
		if err != nil {
			if isNotFound(err) {
				continue
			}
			return nil, convertError("RangeDeleteMessages", err)
		}
		if n, ok := lastOpResultInt(rec, binTaskMap); ok {
			deleted += n
		}
	}

	return &p.InternalRangeDeleteMessagesResponse{MessagesDeleted: deleted}, nil
}

func (s *queueV2Store) ListQueues(
	ctx context.Context,
	request *p.InternalListQueuesRequest,
) (*p.InternalListQueuesResponse, error) {
	if request.PageSize <= 0 {
		return nil, p.ErrNonPositiveListQueuesPageSize
	}

	// Administrative path: a set scan is acceptable here, as it is for
	// namespaces and cluster metadata.
	sp := as.NewScanPolicy()
	sp.TotalTimeout = s.client.cfg.TotalTimeout

	recordset, err := s.client.as.ScanAll(sp, s.client.keys.namespace, s.client.keys.set(setQueueV2Meta))
	if err != nil {
		return nil, convertError("ListQueues", err)
	}
	defer func() { _ = recordset.Close() }()

	var all []p.QueueInfo
	for res := range recordset.Results() {
		if res.Err != nil {
			return nil, convertError("ListQueues", res.Err)
		}
		if p.QueueV2Type(binInt64(res.Record, binQueueType)) != request.QueueType {
			continue
		}
		name := binString(res.Record, binQueueName)
		nextID := binInt64(res.Record, binNextMessageID)

		count, err := s.messageCount(ctx, request.QueueType, name, nextID)
		if err != nil {
			return nil, err
		}
		all = append(all, p.QueueInfo{
			QueueName:     name,
			MessageCount:  count,
			LastMessageID: nextID - 1,
		})
	}

	sortByDigest(all, func(q p.QueueInfo) string { return q.QueueName })

	var cursor []byte
	if len(request.NextPageToken) > 0 {
		var tok queueListToken
		if err := json.Unmarshal(request.NextPageToken, &tok); err != nil {
			return nil, p.ErrInvalidListQueuesNextPageToken
		}
		cursor = []byte(tok.LastQueueName)
	}
	start := pageStart(len(all), cursor, func(i int) string { return all[i].QueueName })
	end := pageEnd(start, request.PageSize, len(all))

	resp := &p.InternalListQueuesResponse{Queues: all[start:end]}

	// A token accompanies any *full* page, even one that happened to exhaust
	// the list. Callers signal the end by getting back a short or empty page,
	// not by getting back an empty token -- returning no token here would make
	// the next call restart from the beginning.
	if end > start && end-start == request.PageSize {
		// Structured, not a bare name: the suite feeds arbitrary text back as a
		// cursor and expects rejection, which a raw string cannot distinguish
		// from a legitimate one.
		resp.NextPageToken, _ = json.Marshal(queueListToken{LastQueueName: all[end-1].QueueName})
	}
	return resp, nil
}

type queueListToken struct {
	LastQueueName string `json:"q"`
}

func (s *queueV2Store) messageCount(
	ctx context.Context, queueType p.QueueV2Type, name string, nextID int64,
) (int64, error) {
	total := int64(0)
	for bucket := int64(0); bucket <= queueBucket(nextID); bucket++ {
		key, err := s.client.keys.queueMessageKey(queueType, name, bucket)
		if err != nil {
			return 0, err
		}
		rec, err := s.client.as.Operate(withCtxW(ctx, s.client.write), key,
			as.MapSizeOp(binTaskMap))
		if err != nil {
			if isNotFound(err) {
				continue
			}
			return 0, convertError("ListQueues", err)
		}
		if n, ok := lastOpResultInt(rec, binTaskMap); ok {
			total += n
		}
	}
	return total, nil
}

// validateStoredEncoding rejects a record whose encoding name is not one
// Temporal recognises, so corruption is reported at the boundary rather than as
// a confusing decode failure several layers up.
func validateStoredEncoding(v any) error {
	parts, ok := v.([]any)
	if !ok || len(parts) < 2 {
		return nil
	}
	enc, _ := parts[1].(string)
	if _, err := enumspb.EncodingTypeFromString(enc); err != nil {
		return serialization.NewUnknownEncodingTypeError(enc)
	}
	return nil
}
