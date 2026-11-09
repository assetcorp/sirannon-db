package sirannon

import (
	"context"
	"fmt"
)

type openSubscription func(ctx context.Context, transport databaseTransport) (closer, error)

type trackedSubscription struct {
	open   openSubscription
	remote closer
	active bool
}

type topologySubscriptions struct {
	owner   *topologyTransport
	gate    chan struct{}
	url     string
	tracked map[*trackedSubscription]struct{}
}

func newTopologySubscriptions(owner *topologyTransport) *topologySubscriptions {
	return &topologySubscriptions{owner: owner, gate: make(chan struct{}, 1), tracked: map[*trackedSubscription]struct{}{}}
}

func (s *topologySubscriptions) acquire(ctx context.Context) error {
	select {
	case s.gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return contextFailure(ctx.Err(), "the subscriptions of "+s.owner.id)
	}
}

func (s *topologySubscriptions) release() {
	<-s.gate
}

func (s *topologySubscriptions) count() int {
	s.owner.mutex.Lock()
	defer s.owner.mutex.Unlock()
	return len(s.tracked)
}

func (s *topologySubscriptions) forgetURL() {
	s.owner.mutex.Lock()
	s.url = ""
	s.owner.mutex.Unlock()
}

func (s *topologySubscriptions) track(ctx context.Context, open openSubscription) (closer, error) {
	handle, err := s.openOnCurrent(ctx, open)
	client := s.owner.client
	if err == nil || ctx.Err() != nil || !client.coordinated() || !shouldRefreshRouting(err) {
		return handle, err
	}
	hadActive := s.count() > 0
	if !hadActive {
		s.forgetURL()
	}
	if refreshErr := client.refreshRouting(ctx, s.owner.id); refreshErr != nil {
		return nil, refreshErr
	}
	if !hadActive {
		s.forgetURL()
	}
	return s.openOnCurrent(ctx, open)
}

func (s *topologySubscriptions) openOnCurrent(ctx context.Context, open openSubscription) (closer, error) {
	candidate, err := s.owner.client.readEndpoint(ctx, s.owner.id, s.owner.client.readConcern)
	if err != nil {
		return nil, err
	}
	if err := s.acquire(ctx); err != nil {
		return nil, err
	}
	defer s.release()
	s.owner.mutex.Lock()
	if s.url == "" {
		s.url = candidate
	}
	url := s.url
	s.owner.mutex.Unlock()
	transport, err := s.owner.endpoint(url)
	if err != nil {
		return nil, err
	}
	remote, err := open(ctx, transport)
	if err != nil {
		return nil, err
	}
	tracked := &trackedSubscription{open: open, remote: remote, active: true}
	s.owner.mutex.Lock()
	if s.owner.closed {
		s.owner.mutex.Unlock()
		_ = remote.unsubscribe(ctx)
		return nil, transportClosed()
	}
	s.tracked[tracked] = struct{}{}
	s.owner.mutex.Unlock()
	return &topologyHandle{set: s, tracked: tracked}, nil
}

func (s *topologySubscriptions) migrate(ctx context.Context) error {
	if !s.owner.client.coordinated() || s.count() == 0 {
		return nil
	}
	url, err := s.owner.client.readEndpoint(ctx, s.owner.id, s.owner.client.readConcern)
	if err != nil {
		return err
	}
	if err := s.acquire(ctx); err != nil {
		return err
	}
	defer s.release()
	s.owner.mutex.Lock()
	current := s.url
	active := make([]*trackedSubscription, 0, len(s.tracked))
	for tracked := range s.tracked {
		if tracked.active {
			active = append(active, tracked)
		}
	}
	s.owner.mutex.Unlock()
	if len(active) == 0 || current == url {
		return nil
	}
	transport, err := s.owner.endpoint(url)
	if err != nil {
		return err
	}
	reopened := make(map[*trackedSubscription]closer, len(active))
	for _, tracked := range active {
		remote, err := tracked.open(ctx, transport)
		if err != nil {
			for _, opened := range reopened {
				_ = opened.unsubscribe(ctx)
			}
			return wrapError(CodeRoutingError, fmt.Sprintf("The client could not move the subscriptions of database %s to the refreshed route: %v", s.owner.id, err), err)
		}
		reopened[tracked] = remote
	}
	var retired []closer
	s.owner.mutex.Lock()
	s.url = url
	for tracked, remote := range reopened {
		if !tracked.active {
			retired = append(retired, remote)
			continue
		}
		if tracked.remote != nil {
			retired = append(retired, tracked.remote)
		}
		tracked.remote = remote
	}
	s.owner.mutex.Unlock()
	for _, remote := range retired {
		_ = remote.unsubscribe(ctx)
	}
	return nil
}

func (s *topologySubscriptions) forgetLocked() {
	for tracked := range s.tracked {
		tracked.active = false
		tracked.remote = nil
	}
	s.tracked = map[*trackedSubscription]struct{}{}
	s.url = ""
}

type topologyHandle struct {
	set     *topologySubscriptions
	tracked *trackedSubscription
}

func (h *topologyHandle) unsubscribe(ctx context.Context) error {
	owner := h.set.owner
	owner.mutex.Lock()
	if !h.tracked.active {
		owner.mutex.Unlock()
		return nil
	}
	h.tracked.active = false
	delete(h.set.tracked, h.tracked)
	remote := h.tracked.remote
	h.tracked.remote = nil
	owner.mutex.Unlock()
	if remote == nil {
		return nil
	}
	return remote.unsubscribe(ctx)
}
