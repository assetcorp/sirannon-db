package sirannon

import (
	"context"
	"strconv"
)

type activeSubscription struct {
	spec        subscriptionSpec
	lastSeq     int64
	hasSeq      bool
	epoch       string
	hasEpoch    bool
	established bool
}

type activeLive struct {
	spec        liveSpec
	established bool
}

type restoreAttempt struct {
	done chan struct{}
	err  error
}

func (t *wsTransport) subscribeFrameLocked(id string, subscription *activeSubscription) WSSubscribeMessage {
	frame := WSSubscribeMessage{Type: "subscribe", ID: id, Table: Ptr(subscription.spec.table), Filter: subscription.spec.filter}
	if subscription.hasSeq {
		frame.SinceSeq = Ptr(strconv.FormatInt(subscription.lastSeq, 10))
	}
	if subscription.hasEpoch {
		frame.Epoch = Ptr(subscription.epoch)
	}
	return frame
}

func (t *wsTransport) subscribe(ctx context.Context, spec subscriptionSpec) (closer, error) {
	if _, err := t.connection(ctx, true); err != nil {
		return nil, err
	}
	id := t.nextID()
	subscription := &activeSubscription{spec: spec}
	t.mutex.Lock()
	if t.closed {
		t.mutex.Unlock()
		return nil, transportClosed()
	}
	t.subscriptions[id] = subscription
	frame := t.subscribeFrameLocked(id, subscription)
	t.mutex.Unlock()
	if _, err := t.request(ctx, id, frame, true); err != nil {
		t.dropSubscription(id, subscription)
		return nil, err
	}
	return &wsHandle{transport: t, id: id, subscription: subscription}, nil
}

func (t *wsTransport) dropSubscription(id string, subscription *activeSubscription) bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if t.subscriptions[id] != subscription {
		return false
	}
	delete(t.subscriptions, id)
	return true
}

func (t *wsTransport) dropLive(id string, live *activeLive) bool {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	if t.lives[id] != live {
		return false
	}
	delete(t.lives, id)
	return true
}

func (t *wsTransport) sendLive(ctx context.Context, id string, live *activeLive) error {
	digest, err := live.spec.digest(ctx, false)
	if err != nil {
		return err
	}
	frame := WSSubscribeMessage{Type: "subscribe", ID: id, Name: Ptr(live.spec.name), Args: live.spec.args}
	if digest != "" {
		frame.RegistryDigest = Ptr(digest)
	}
	_, err = t.request(ctx, id, frame, true)
	if ErrorCode(err) == codeRegistryMismatch {
		digest, err = live.spec.digest(ctx, true)
		if err != nil {
			return err
		}
		frame.RegistryDigest = nil
		if digest != "" {
			frame.RegistryDigest = Ptr(digest)
		}
		_, err = t.request(ctx, id, frame, true)
	}
	if err != nil {
		return err
	}
	t.mutex.Lock()
	live.established = true
	t.mutex.Unlock()
	return nil
}

func (t *wsTransport) live(ctx context.Context, spec liveSpec) (closer, error) {
	if _, err := t.connection(ctx, true); err != nil {
		return nil, err
	}
	id := t.nextID()
	live := &activeLive{spec: spec}
	t.mutex.Lock()
	if t.closed {
		t.mutex.Unlock()
		return nil, transportClosed()
	}
	t.lives[id] = live
	t.mutex.Unlock()
	if err := t.sendLive(ctx, id, live); err != nil {
		t.dropLive(id, live)
		return nil, err
	}
	return &wsHandle{transport: t, id: id, live: live}, nil
}

func unreachableCode(err error) bool {
	code := ErrorCode(err)
	return code == CodeConnectionError || code == CodeTimeout
}

func (t *wsTransport) restore(ctx context.Context) error {
	t.mutex.Lock()
	attempt := t.restoring
	if attempt == nil {
		if !t.needsRestore {
			t.mutex.Unlock()
			return nil
		}
		attempt = &restoreAttempt{done: make(chan struct{})}
		t.needsRestore, t.restoring = false, attempt
		if !t.tasks.start(func() { t.restoreAll(attempt) }) {
			t.restoring = nil
			t.mutex.Unlock()
			return transportClosed()
		}
	}
	t.mutex.Unlock()
	select {
	case <-attempt.done:
		return attempt.err
	case <-ctx.Done():
		return contextFailure(ctx.Err(), t.url)
	}
}

func (t *wsTransport) restoreAll(attempt *restoreAttempt) {
	ctx := t.lifetime
	t.mutex.Lock()
	subscriptionIDs := map[string]*activeSubscription{}
	for id, subscription := range t.subscriptions {
		if !subscription.established {
			subscriptionIDs[id] = subscription
		}
	}
	liveIDs := map[string]*activeLive{}
	for id, live := range t.lives {
		if !live.established {
			liveIDs[id] = live
		}
	}
	t.mutex.Unlock()
	var unreachable error
	for id, subscription := range subscriptionIDs {
		if err := t.restoreSubscription(ctx, id, subscription); err != nil {
			unreachable = err
		}
	}
	for id, live := range liveIDs {
		err := t.sendLive(ctx, id, live)
		switch {
		case err == nil:
		case unreachableCode(err) || ErrorCode(err) == CodeTransportError:
			unreachable = err
		default:
			if t.dropLive(id, live) {
				live.spec.handlers.fail(err)
			}
		}
	}
	t.mutex.Lock()
	t.restoring = nil
	if unreachable != nil && !t.closed {
		t.needsRestore = true
	}
	attempt.err = unreachable
	t.mutex.Unlock()
	close(attempt.done)
}

func (t *wsTransport) restoreSubscription(ctx context.Context, id string, subscription *activeSubscription) error {
	t.mutex.Lock()
	if t.subscriptions[id] != subscription {
		t.mutex.Unlock()
		return nil
	}
	frame := t.subscribeFrameLocked(id, subscription)
	t.mutex.Unlock()
	_, err := t.request(ctx, id, frame, true)
	switch {
	case err == nil:
		return nil
	case unreachableCode(err) || ErrorCode(err) == CodeTransportError:
		return err
	case ErrorCode(err) == codeDuplicateSubscription:
		t.mutex.Lock()
		subscription.established = true
		t.mutex.Unlock()
		return nil
	}
	if t.dropSubscription(id, subscription) {
		onError := subscription.spec.onError
		t.events.post(func() { reportFailure(onError, err) })
	}
	return nil
}

func (t *wsTransport) ack(ctx context.Context, deviceID string, seq int64) (*AckResponse, error) {
	if _, err := t.connection(ctx, true); err != nil {
		return nil, err
	}
	if err := t.restore(ctx); err != nil {
		return nil, err
	}
	id := t.nextID()
	data, err := t.request(ctx, id, WSAckMessage{Type: "ack", ID: id, DeviceID: deviceID, Seq: strconv.FormatInt(seq, 10)}, true)
	if err != nil {
		return nil, err
	}
	var answer AckResponse
	if err := decodeData(data, &answer); err != nil {
		return nil, err
	}
	return &answer, nil
}

type wsHandle struct {
	transport    *wsTransport
	id           string
	subscription *activeSubscription
	live         *activeLive
}

func (h *wsHandle) unsubscribe(ctx context.Context) error {
	removed := false
	if h.subscription != nil {
		removed = h.transport.dropSubscription(h.id, h.subscription)
	}
	if h.live != nil {
		removed = h.transport.dropLive(h.id, h.live)
	}
	if !removed {
		return nil
	}
	_, err := h.transport.request(ctx, h.id, WSUnsubscribeMessage{Type: "unsubscribe", ID: h.id}, false)
	if ErrorCode(err) == CodeTransportError {
		return nil
	}
	return err
}
