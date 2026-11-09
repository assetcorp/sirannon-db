package sirannon

import (
	"fmt"
	"runtime/debug"
	"sync"
)

type taskGroup struct {
	mutex   sync.Mutex
	idle    *sync.Cond
	closed  bool
	running int
}

func newTaskGroup() *taskGroup {
	group := &taskGroup{}
	group.idle = sync.NewCond(&group.mutex)
	return group
}

func (g *taskGroup) start(task func()) bool {
	g.mutex.Lock()
	if g.closed {
		g.mutex.Unlock()
		return false
	}
	g.running++
	g.mutex.Unlock()
	go func() {
		defer g.finish()
		task()
	}()
	return true
}

func (g *taskGroup) finish() {
	g.mutex.Lock()
	g.running--
	if g.running == 0 {
		g.idle.Broadcast()
	}
	g.mutex.Unlock()
}

func (g *taskGroup) closeAndWait() {
	g.mutex.Lock()
	g.closed = true
	for g.running > 0 {
		g.idle.Wait()
	}
	g.mutex.Unlock()
}

func invokeCallback(onError func(error), callback func()) {
	defer func() {
		recovered := recover()
		if recovered == nil {
			return
		}
		reportFailure(onError, &CallbackPanic{Value: recovered, Stack: debug.Stack()})
	}()
	callback()
}

func reportFailure(onError func(error), failure error) {
	if onError == nil {
		return
	}
	defer func() {
		_ = recover()
	}()
	onError(failure)
}

type CallbackPanic struct {
	Value any
	Stack []byte
}

func (p *CallbackPanic) Error() string {
	return fmt.Sprintf("a callback panicked: %v", p.Value)
}
