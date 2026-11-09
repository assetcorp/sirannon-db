package sirannon

import (
	"context"
	"sync"
)

const callbackBacklogLimit = 1024

type dispatcher struct {
	mutex    sync.Mutex
	queue    []func()
	pending  int
	wake     chan struct{}
	progress chan struct{}
	stopped  bool
}

func startDispatcher(lifetime context.Context) *dispatcher {
	d := &dispatcher{wake: make(chan struct{}, 1), progress: make(chan struct{}, 1)}
	go d.run(lifetime)
	return d
}

func (d *dispatcher) post(task func()) {
	d.mutex.Lock()
	if d.stopped {
		d.mutex.Unlock()
		return
	}
	d.queue = append(d.queue, task)
	d.pending++
	d.mutex.Unlock()
	signal(d.wake)
}

func (d *dispatcher) take() []func() {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	taken := d.queue
	d.queue = nil
	return taken
}

func (d *dispatcher) finished() {
	d.mutex.Lock()
	if d.pending > 0 {
		d.pending--
	}
	d.mutex.Unlock()
	signal(d.progress)
}

func (d *dispatcher) backlogged() bool {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	return !d.stopped && d.pending >= callbackBacklogLimit
}

func (d *dispatcher) paced(ctx context.Context) {
	for d.backlogged() {
		select {
		case <-ctx.Done():
			return
		case <-d.progress:
		}
	}
}

func (d *dispatcher) run(lifetime context.Context) {
	defer func() {
		d.mutex.Lock()
		d.stopped = true
		d.queue = nil
		d.pending = 0
		d.mutex.Unlock()
		signal(d.progress)
	}()
	for {
		for _, task := range d.take() {
			if lifetime.Err() != nil {
				return
			}
			task()
			d.finished()
		}
		select {
		case <-lifetime.Done():
			return
		case <-d.wake:
		}
	}
}

func signal(channel chan struct{}) {
	select {
	case channel <- struct{}{}:
	default:
	}
}
