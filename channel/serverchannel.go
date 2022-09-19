package channel

import (
	"net"
	"sync"

	concurrent "github.com/kklab-com/goth-concurrent"
)

type ServerChannel interface {
	Channel
	setChildHandler(handler Handler) ServerChannel
	setChildParams(key ParamKey, value any)
	ChildParams() *Params
	releaseChild(channel Channel)
	waitChildren()
}

type DefaultServerChannel struct {
	DefaultChannel
	childHandler Handler
	childParams  Params
	childMap     sync.Map
}

func (c *DefaultServerChannel) activeChannel() {
	scp := c
	scp.DefaultChannel.activeChannel()
	scp.DefaultChannel.alive.Chainable().Then(func(parent concurrent.Future) any {
		scp.childMap.Range(func(key, value any) bool {
			if ch, ok := value.(Channel); ok {
				if ch.IsActive() {
					ch.inactiveChannel()
				}
			}
			return true
		})

		return parent.Get()
	})
}

func (c *DefaultServerChannel) setChildHandler(handler Handler) ServerChannel {
	c.childHandler = handler
	return c
}

func (c *DefaultServerChannel) setChildParams(key ParamKey, value any) {
	c.childParams.Store(key, value)
}

func (c *DefaultServerChannel) waitChildren() {
	c.childMap.Range(func(key, value any) bool {
		ch := value.(Channel)
		ch.CloseFuture().Await()
		return true
	})
}

func (c *DefaultServerChannel) ChildParams() *Params {
	return &c.childParams
}

func (c *DefaultServerChannel) releaseChild(channel Channel) {
	c.childMap.Delete(channel.Serial())
}

func (c *DefaultServerChannel) DeriveChildChannel(child Channel, parent ServerChannel) Channel {
	child.init(child)
	child.setParent(parent)
	c.childMap.Store(child.Serial(), child)
	c.ChildParams().Range(func(k ParamKey, v any) bool {
		child.SetParam(k, v)
		return true
	})

	child.Init()
	if c.childHandler != nil {
		child.Pipeline().AddLast("ROOT", c.childHandler)
	}

	return child
}

func (c *DefaultServerChannel) UnsafeBind(localAddr net.Addr) error {
	return nil
}

func (c *DefaultServerChannel) UnsafeAccept() (Channel, Future) {
	return nil, c.Pipeline().NewFuture()
}

func (c *DefaultServerChannel) UnsafeRead() (any, error) {
	return nil, nil
}

func (c *DefaultServerChannel) UnsafeClose() error {
	return nil
}

func (c *DefaultServerChannel) UnsafeIsAutoRead() bool {
	return false
}
