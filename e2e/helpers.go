package e2e

import (
	"errors"

	workflowservice "go.temporal.io/api/workflowservice/v1"
)

func describeNamespaceRequest(namespace string) *workflowservice.DescribeNamespaceRequest {
	return &workflowservice.DescribeNamespaceRequest{Namespace: namespace}
}

func asNamespaceNotFound(err error, target any) bool {
	return errors.As(err, target)
}
