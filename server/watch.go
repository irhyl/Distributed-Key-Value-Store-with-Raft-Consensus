// watch.go — change-data-capture: streams every committed write to
// subscribed clients in commit order, via the Watch gRPC RPC.
//
// No pub/sub or fan-out mechanism exists elsewhere in this codebase —
// raft.Node.ApplyCh and StateMachine.pending are both single-consumer — so
// changeBroadcaster is new infrastructure, not a reuse of an existing
// pattern.

package server

import (
	"strings"
	"sync"

	pb "github.com/raftkv/proto"
)

// watchSubscriberBuffer bounds how far a single Watch subscriber can lag
// behind the apply loop before it's dropped.
const watchSubscriberBuffer = 256

// watchSubscriber is one active Watch RPC's delivery channel.
type watchSubscriber struct {
	keyPrefix string
	ch        chan *pb.ChangeEvent
}

// changeBroadcaster fans out applied writes to any number of Watch
// subscribers.
type changeBroadcaster struct {
	mu   sync.Mutex
	subs map[*watchSubscriber]struct{}
}

func newChangeBroadcaster() *changeBroadcaster {
	return &changeBroadcaster{subs: make(map[*watchSubscriber]struct{})}
}

// subscribe registers a new subscriber and returns it, plus an unsubscribe
// function the caller must call (typically via defer) when the Watch RPC
// ends.
func (b *changeBroadcaster) subscribe(keyPrefix string) (*watchSubscriber, func()) {
	sub := &watchSubscriber{
		keyPrefix: keyPrefix,
		ch:        make(chan *pb.ChangeEvent, watchSubscriberBuffer),
	}
	b.mu.Lock()
	b.subs[sub] = struct{}{}
	b.mu.Unlock()

	return sub, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if _, ok := b.subs[sub]; ok {
			delete(b.subs, sub)
			close(sub.ch)
		}
	}
}

// publish delivers event to every subscriber whose key_prefix matches (or
// all subscribers, if a subscriber didn't set one). Called synchronously
// from the apply loop, so this must never block on a slow consumer: a
// subscriber whose buffer is already full is disconnected — its Watch RPC
// ends with an error rather than being allowed to stall the apply loop (and
// therefore every other write in the cluster) waiting for it to catch up.
// The client is expected to reconnect and, if it needs to catch up on what
// it missed, fall back to Get on the keys it cares about.
func (b *changeBroadcaster) publish(event *pb.ChangeEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for sub := range b.subs {
		if sub.keyPrefix != "" && !strings.HasPrefix(event.Key, sub.keyPrefix) {
			continue
		}
		select {
		case sub.ch <- event:
		default:
			delete(b.subs, sub)
			close(sub.ch)
		}
	}
}
