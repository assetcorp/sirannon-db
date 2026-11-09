package sirannon

import (
	"context"
	"maps"
	"sync"
)

// SubscriptionBuilder sets up a subscription to the changes to one table. [RemoteDatabase.On] returns
// one.
type SubscriptionBuilder struct {
	database *RemoteDatabase
	table    string
	filter   map[string]any
}

// Filter returns a builder whose subscription delivers only the changes to rows that hold the
// filter's value under every key. The server checks the row both before and after each change.
func (b *SubscriptionBuilder) Filter(filter map[string]any) *SubscriptionBuilder {
	return &SubscriptionBuilder{database: b.database, table: b.table, filter: maps.Clone(filter)}
}

// SubscribeOption changes one subscription or one live query.
type SubscribeOption func(*subscribeOptions)

type subscribeOptions struct {
	onError func(error)
	onReset func()
}

// OnError sets the handler that receives each failure that happens outside any call, such as a
// refused reconnection or a [*CallbackPanic] from one of your callbacks.
func OnError(handler func(error)) SubscribeOption {
	return func(options *subscribeOptions) {
		options.onError = handler
	}
}

// OnReset sets the handler that the client calls when the server has pruned the history after the
// last change that the subscription delivered, so read the table again in that handler. Only a
// subscription calls this handler.
func OnReset(handler func()) SubscribeOption {
	return func(options *subscribeOptions) {
		options.onReset = handler
	}
}

func resolveSubscribeOptions(options []SubscribeOption) subscribeOptions {
	var resolved subscribeOptions
	for _, option := range options {
		if option != nil {
			option(&resolved)
		}
	}
	return resolved
}

// Subscribe starts the subscription and returns once the server confirms it. The client calls
// callback once for each change, in seq order, on a goroutine apart from the one that reads the
// connection. Subscribe needs the WebSocket transport. It fails with INVALID_ARGUMENT for an empty
// table name or a nil callback.
func (b *SubscriptionBuilder) Subscribe(ctx context.Context, callback func(ChangeEvent), options ...SubscribeOption) (*Subscription, error) {
	if b.table == "" {
		return nil, invalidArgument("A subscription needs a table name")
	}
	if callback == nil {
		return nil, invalidArgument("A subscription needs a callback")
	}
	resolved := resolveSubscribeOptions(options)
	spec := subscriptionSpec{
		table:    b.table,
		filter:   maps.Clone(b.filter),
		callback: callback,
		onError:  resolved.onError,
		onReset:  resolved.onReset,
	}
	var handle closer
	err := b.database.run(ctx, func(ctx context.Context) error {
		var err error
		handle, err = b.database.transport.subscribe(ctx, spec)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &Subscription{handle: handle}, nil
}

// Subscription is an open subscription, which [Subscription.Unsubscribe] stops.
type Subscription struct {
	handle closer
	once   sync.Once
	err    error
}

// Unsubscribe stops the subscription and tells the server to stop sending its changes. A later call
// returns the error that the first call returned.
func (s *Subscription) Unsubscribe(ctx context.Context) error {
	s.once.Do(func() {
		s.err = s.handle.unsubscribe(ctx)
	})
	return s.err
}
