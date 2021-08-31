package channel

import (
	"context"
	"net"
)

type ServerChannel interface {
	Channel
	setChildHandler(handler Handler) ServerChannel
	setChildParams(key ParamKey, value interface{})
	ChildParams() *Params
}

type DefaultServerChannel struct {
	DefaultChannel
	childHandler Handler
	childParams  Params
}

func (c *DefaultServerChannel) setChildHandler(handler Handler) ServerChannel {
	c.childHandler = handler
	return c
}

func (c *DefaultServerChannel) setChildParams(key ParamKey, value interface{}) {
	c.childParams.Store(key, value)
}

func (c *DefaultServerChannel) ChildParams() *Params {
	return &c.childParams
}

func (c *DefaultServerChannel) DeriveChildChannel(child Channel, parent ServerChannel) Channel {
	child.setPipeline(_NewDefaultPipeline(child))
	child.setParent(parent)
	cancel, cancelFunc := context.WithCancel(c.ctx)
	child.setContext(cancel)
	child.setContextCancelFunc(cancelFunc)
	c.ChildParams().Range(func(k ParamKey, v interface{}) bool {
		child.SetParam(k, v)
		return true
	})

	child.Init()
	if c.childHandler != nil {
		child.Pipeline().AddLast("ROOT", c.childHandler)
	}

	child.setCloseFuture(child.Pipeline().NewFuture())
	c.closeWG.Add(1)
	return child
}

func (c *DefaultServerChannel) UnsafeBind(localAddr net.Addr) error {
	return nil
}

func (c *DefaultServerChannel) UnsafeAccept() (Channel, Future) {
	return nil, c.pipeline.NewFuture()
}

func (c *DefaultServerChannel) UnsafeClose() error {
	return nil
}

func (c *DefaultServerChannel) UnsafeIsAutoRead() bool {
	return false
}
