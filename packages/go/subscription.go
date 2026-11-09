package sirannon

import (
	"context"
	"maps"
	"sync"
)

type SubscriptionBuilder struct {
	database *RemoteDatabase
	table    string
	filter   map[string]any
}

func (b *SubscriptionBuilder) Filter(filter map[string]any) *SubscriptionBuilder {
	return &SubscriptionBuilder{database: b.database, table: b.table, filter: maps.Clone(filter)}
}

type SubscribeOption func(*subscribeOptions)

type subscribeOptions struct {
	onError func(error)
	onReset func()
}

func OnError(handler func(error)) SubscribeOption {
	return func(options *subscribeOptions) {
		options.onError = handler
	}
}

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

type Subscription struct {
	handle closer
	once   sync.Once
	err    error
}

func (s *Subscription) Unsubscribe(ctx context.Context) error {
	s.once.Do(func() {
		s.err = s.handle.unsubscribe(ctx)
	})
	return s.err
}
