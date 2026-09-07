package control

import (
	"fmt"
	"sync"
	"time"
)

// EventType is the discriminator the UI switches on.
const (
	EventProgress = "progress"
	EventState    = "state"
	EventError    = "error"
)

// Event is one line of the live log the UI renders during a switch.
//
// Message is always human-readable prose, because every event ends up rendered
// in that log. A state event carries the structured payload alongside it in
// State, so the UI can refresh its buttons from the same stream that feeds the
// log instead of polling /api/state next to it.
type Event struct {
	Type    string    `json:"type"`
	Message string    `json:"message"`
	Ts      time.Time `json:"ts"`
	State   *State    `json:"state,omitempty"`
}

func newStateEvent(state State) Event {
	msg := fmt.Sprintf("backend %s, temporal %s", state.Backend, readiness(state.TemporalReady))
	if state.Switching {
		msg = fmt.Sprintf("switching to %s", state.Backend)
	}
	if state.Error != "" {
		msg += " (" + state.Error + ")"
	}
	return Event{Type: EventState, Message: msg, Ts: time.Now(), State: &state}
}

func readiness(ready bool) string {
	if ready {
		return "ready"
	}
	return "not ready"
}

// sseHub fans one publisher out to every connected browser tab.
//
// Subscribers are buffered and lossy on purpose. A switch publishes from the
// goroutine driving the rollout, and a tab that has been backgrounded by the OS
// must not be able to block it -- dropping a progress line for a client that is
// not reading is strictly better than stalling the demo for everyone.
type sseHub struct {
	mu     sync.Mutex
	subs   map[chan Event]struct{}
	closed bool
}

// subBuffer is sized for a whole switch's worth of progress lines, so a client
// briefly behind catches up rather than losing the narrative.
const subBuffer = 64

func newSSEHub() *sseHub {
	return &sseHub{subs: make(map[chan Event]struct{})}
}

func (h *sseHub) subscribe() chan Event {
	ch := make(chan Event, subBuffer)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		close(ch)
		return ch
	}
	h.subs[ch] = struct{}{}
	return ch
}

func (h *sseHub) unsubscribe(ch chan Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.subs[ch]; !ok {
		return
	}
	delete(h.subs, ch)
	close(ch)
}

func (h *sseHub) publish(ev Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs {
		select {
		case ch <- ev:
		default:
		}
	}
}

func (h *sseHub) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	for ch := range h.subs {
		delete(h.subs, ch)
		close(ch)
	}
}
