package sirannon

import (
	"context"
	"slices"
	"sync"
	"time"
)

const (
	capabilitySQL               = "query.sql"
	defaultCapabilityReadWindow = 30 * time.Second
)

type capabilityReport struct {
	capabilities []string
	digest       string
	acceptsSQL   bool
}

type capabilityFetch struct {
	done   chan struct{}
	report *capabilityReport
	err    error
}

type capabilityCache struct {
	transport *httpTransport
	tasks     *taskGroup
	mutex     sync.Mutex
	report    *capabilityReport
	inflight  *capabilityFetch
}

func newCapabilityCache(transport *httpTransport, tasks *taskGroup) *capabilityCache {
	return &capabilityCache{transport: transport, tasks: tasks}
}

func (c *capabilityCache) fetch(attempt *capabilityFetch) {
	window := c.transport.settings.requestTimeout
	if window <= 0 {
		window = defaultCapabilityReadWindow
	}
	ctx, cancel := context.WithTimeout(c.transport.lifetime, window)
	defer cancel()
	var answer CapabilitiesResponse
	err := c.transport.getJSON(ctx, "/capabilities", &answer)
	var report *capabilityReport
	switch {
	case err == nil:
		report = &capabilityReport{
			capabilities: answer.Capabilities,
			acceptsSQL:   slices.Contains(answer.Capabilities, capabilitySQL),
		}
		if answer.Registry != nil {
			report.digest = answer.Registry.Digest
		}
	case ErrorCode(err) == codeNotFound:
		report = &capabilityReport{acceptsSQL: true}
		err = nil
	}
	c.mutex.Lock()
	if c.inflight == attempt {
		c.inflight = nil
		if report != nil {
			c.report = report
		}
	}
	c.mutex.Unlock()
	attempt.report, attempt.err = report, err
	close(attempt.done)
}

func (c *capabilityCache) read(ctx context.Context, refresh bool, closed <-chan struct{}) (*capabilityReport, error) {
	c.mutex.Lock()
	if refresh {
		c.report = nil
	}
	if c.report != nil {
		report := c.report
		c.mutex.Unlock()
		return report, nil
	}
	attempt := c.inflight
	if attempt == nil {
		attempt = &capabilityFetch{done: make(chan struct{})}
		if !c.tasks.start(func() { c.fetch(attempt) }) {
			c.mutex.Unlock()
			return nil, transportClosed()
		}
		c.inflight = attempt
	}
	c.mutex.Unlock()
	select {
	case <-attempt.done:
		return attempt.report, attempt.err
	case <-closed:
		return nil, transportClosed()
	case <-c.transport.lifetime.Done():
		return nil, transportClosed()
	case <-ctx.Done():
		return nil, contextFailure(ctx.Err(), "GET "+c.transport.base+"/capabilities")
	}
}

func (c *capabilityCache) requireSQL(ctx context.Context, closed <-chan struct{}) error {
	report, err := c.read(ctx, false, closed)
	if err != nil {
		return err
	}
	if !report.acceptsSQL {
		return newError(CodeSQLNotAccepted, sqlRefusedMessage)
	}
	return nil
}

func (c *capabilityCache) registryDigest(ctx context.Context, refresh bool, closed <-chan struct{}) (string, error) {
	report, err := c.read(ctx, refresh, closed)
	if err != nil {
		return "", err
	}
	return report.digest, nil
}
