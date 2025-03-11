package channel

import (
	"context"
	"github.com/kklab-com/goth-kklogger"
	kkpanic "github.com/kklab-com/goth-panic"
	"net"
	"time"
)

type HandlerContext interface {
	context.Context
	WithValue(key, val any) HandlerContext
	Name() string
	Channel() Channel
	FireRegistered() HandlerContext
	FireUnregistered() HandlerContext
	FireActive() HandlerContext
	FireInactive() HandlerContext
	FireRead(obj any) HandlerContext
	FireReadCompleted() HandlerContext
	FireErrorCaught(err error) HandlerContext
	Write(obj any, future Future) Future
	Bind(localAddr net.Addr, future Future) Future
	Close(future Future) Future
	Connect(localAddr net.Addr, remoteAddr net.Addr, future Future) Future
	Disconnect(future Future) Future
	Deregister(future Future) Future
	prev() HandlerContext
	setPrev(prev HandlerContext) HandlerContext
	next() HandlerContext
	setNext(prev HandlerContext) HandlerContext
	deferErrorCaught()
	checkFuture(future Future) Future
	handler() Handler
	_Context() context.Context
}

type wrapHandlerContext struct {
	HandlerContext
	ctx context.Context
}

func (c *wrapHandlerContext) _Context() context.Context {
	return c.ctx
}

func (c *wrapHandlerContext) FireRegistered() HandlerContext {
	if c.next() != nil {
		defer c.next().deferErrorCaught()
		c.next().handler().Registered(_NewWrapHandlerContext(c._Context(), c.next()))
	}

	return c
}

func (c *wrapHandlerContext) FireUnregistered() HandlerContext {
	if c.next() != nil {
		defer c.next().deferErrorCaught()
		c.next().handler().Unregistered(_NewWrapHandlerContext(c._Context(), c.next()))
	}

	return c
}

func (c *wrapHandlerContext) FireActive() HandlerContext {
	if c.next() != nil {
		defer c.next().deferErrorCaught()
		c.next().handler().Active(_NewWrapHandlerContext(c._Context(), c.next()))
	}

	return c
}

func (c *wrapHandlerContext) FireInactive() HandlerContext {
	if c.next() != nil {
		defer c.next().deferErrorCaught()
		c.next().handler().Inactive(_NewWrapHandlerContext(c._Context(), c.next()))
	}

	return c
}

func (c *wrapHandlerContext) FireRead(obj any) HandlerContext {
	if c.next() != nil {
		defer c.next().deferErrorCaught()
		c.next().handler().Read(_NewWrapHandlerContext(c._Context(), c.next()), obj)
	}

	return c
}

func (c *wrapHandlerContext) FireReadCompleted() HandlerContext {
	if c.next() != nil {
		defer c.next().deferErrorCaught()
		c.next().handler().ReadCompleted(_NewWrapHandlerContext(c._Context(), c.next()))
	}

	return c
}

func (c *wrapHandlerContext) FireErrorCaught(err error) HandlerContext {
	if c.prev() != nil {
		defer c.prev().deferErrorCaught()
		c.prev().handler().ErrorCaught(_NewWrapHandlerContext(c._Context(), c.prev()), err)
	}

	return c
}

func (c *wrapHandlerContext) Write(obj any, future Future) Future {
	future = c.checkFuture(future)
	if c.prev() != nil {
		defer c.prev().deferErrorCaught()
		c.prev().handler().Write(_NewWrapHandlerContext(c._Context(), c.prev()), obj, future)
	}

	return future
}

func (c *wrapHandlerContext) Bind(localAddr net.Addr, future Future) Future {
	future = c.checkFuture(future)
	if c.prev() != nil {
		defer c.prev().deferErrorCaught()
		c.prev().handler().Bind(_NewWrapHandlerContext(c._Context(), c.prev()), localAddr, future)
	}

	return future
}

func (c *wrapHandlerContext) Close(future Future) Future {
	future = c.checkFuture(future)
	if c.prev() != nil {
		defer c.prev().deferErrorCaught()
		c.prev().handler().Close(_NewWrapHandlerContext(c._Context(), c.prev()), future)
	}

	return future
}

func (c *wrapHandlerContext) Connect(localAddr net.Addr, remoteAddr net.Addr, future Future) Future {
	future = c.checkFuture(future)
	if c.prev() != nil {
		defer c.prev().deferErrorCaught()
		c.prev().handler().Connect(_NewWrapHandlerContext(c._Context(), c.prev()), localAddr, remoteAddr, future)
	}

	return future
}

func (c *wrapHandlerContext) Disconnect(future Future) Future {
	future = c.checkFuture(future)
	if c.prev() != nil {
		defer c.prev().deferErrorCaught()
		c.prev().handler().Disconnect(_NewWrapHandlerContext(c._Context(), c.prev()), future)
	}

	return future
}

func (c *wrapHandlerContext) Deregister(future Future) Future {
	future = c.checkFuture(future)
	if c.next() != nil {
		defer c.next().deferErrorCaught()
		c.next().handler().Deregister(_NewWrapHandlerContext(c._Context(), c.next()), future)
	}

	return future
}

func (c *wrapHandlerContext) Deadline() (deadline time.Time, ok bool) {
	return c._Context().Deadline()
}

func (c *wrapHandlerContext) Done() <-chan struct{} {
	return c._Context().Done()
}

func (c *wrapHandlerContext) Err() error {
	return c._Context().Err()
}

func (c *wrapHandlerContext) Value(key any) any {
	return c._Context().Value(key)
}

func (c *wrapHandlerContext) WithValue(key, val any) HandlerContext {
	return &wrapHandlerContext{
		HandlerContext: c,
		ctx:            context.WithValue(c._Context(), key, val),
	}
}

func _NewWrapHandlerContext(parent context.Context, handlerContext HandlerContext) HandlerContext {
	if parent == nil {
		parent = context.Background()
	}

	if handlerContext == nil {
		panic("nil handlerContext")
	}

	return &wrapHandlerContext{
		HandlerContext: handlerContext,
		ctx:            context.WithValue(parent, "handlerContext", handlerContext),
	}
}

type ValueHandlerContext wrapHandlerContext

type DefaultHandlerContext struct {
	name     string
	pipeline Pipeline
	_handler Handler
	nextCtx  HandlerContext
	prevCtx  HandlerContext
	ctx      context.Context
}

func (c *DefaultHandlerContext) setPrev(prev HandlerContext) HandlerContext {
	c.prevCtx = prev
	return c
}

func (c *DefaultHandlerContext) next() HandlerContext {
	return c.nextCtx
}

func (c *DefaultHandlerContext) setNext(next HandlerContext) HandlerContext {
	c.nextCtx = next
	return c
}

func (c *DefaultHandlerContext) handler() Handler {
	return c._handler
}

func (c *DefaultHandlerContext) _Context() context.Context {
	if c.ctx == nil {
		return context.Background()
	}

	return c.ctx
}

func NewHandlerContext() *DefaultHandlerContext {
	c := new(DefaultHandlerContext)
	return c
}

func (c *DefaultHandlerContext) Deadline() (deadline time.Time, ok bool) {
	return c._Context().Deadline()
}

func (c *DefaultHandlerContext) Done() <-chan struct{} {
	return c._Context().Done()
}

func (c *DefaultHandlerContext) Err() error {
	return c._Context().Err()
}

func (c *DefaultHandlerContext) Value(key any) any {
	return c._Context().Value(key)
}

func (c *DefaultHandlerContext) WithValue(key, val any) HandlerContext {
	return &ValueHandlerContext{
		HandlerContext: c,
		ctx:            context.WithValue(c._Context(), key, val),
	}
}

func (c *DefaultHandlerContext) Name() string {
	return c.name
}

func (c *DefaultHandlerContext) Channel() Channel {
	return c.pipeline.Channel()
}

func (c *DefaultHandlerContext) FireRegistered() HandlerContext {
	if c.next() != nil {
		defer c.next().deferErrorCaught()
		c.next().handler().Registered(_NewWrapHandlerContext(c._Context(), c.next()))
	}

	return c
}

func (c *DefaultHandlerContext) FireUnregistered() HandlerContext {
	if c.next() != nil {
		defer c.next().deferErrorCaught()
		c.next().handler().Unregistered(_NewWrapHandlerContext(c._Context(), c.next()))
	}

	return c
}

func (c *DefaultHandlerContext) FireActive() HandlerContext {
	if c.next() != nil {
		defer c.next().deferErrorCaught()
		c.next().handler().Active(_NewWrapHandlerContext(c._Context(), c.next()))
	}

	return c
}

func (c *DefaultHandlerContext) FireInactive() HandlerContext {
	if c.next() != nil {
		defer c.next().deferErrorCaught()
		c.next().handler().Inactive(_NewWrapHandlerContext(c._Context(), c.next()))
	}

	return c
}

func (c *DefaultHandlerContext) FireRead(obj any) HandlerContext {
	if c.next() != nil {
		defer c.next().deferErrorCaught()
		c.next().handler().Read(_NewWrapHandlerContext(c._Context(), c.next()), obj)
	}

	return c
}

func (c *DefaultHandlerContext) FireReadCompleted() HandlerContext {
	if c.next() != nil {
		defer c.next().deferErrorCaught()
		c.next().handler().ReadCompleted(_NewWrapHandlerContext(c._Context(), c.next()))
	}

	return c
}

func (c *DefaultHandlerContext) FireErrorCaught(err error) HandlerContext {
	if c.prev() != nil {
		defer c.prev().deferErrorCaught()
		c.prev().handler().ErrorCaught(_NewWrapHandlerContext(c._Context(), c.prev()), err)
	}

	return c
}

func (c *DefaultHandlerContext) Write(obj any, future Future) Future {
	future = c.checkFuture(future)
	if c.prev() != nil {
		defer c.prev().deferErrorCaught()
		c.prev().handler().Write(_NewWrapHandlerContext(c._Context(), c.prev()), obj, future)
	}

	return future
}

func (c *DefaultHandlerContext) Bind(localAddr net.Addr, future Future) Future {
	future = c.checkFuture(future)
	if c.prev() != nil {
		defer c.prev().deferErrorCaught()
		c.prev().handler().Bind(_NewWrapHandlerContext(c._Context(), c.prev()), localAddr, future)
	}

	return future
}

func (c *DefaultHandlerContext) Close(future Future) Future {
	future = c.checkFuture(future)
	if c.prev() != nil {
		defer c.prev().deferErrorCaught()
		c.prev().handler().Close(_NewWrapHandlerContext(c._Context(), c.prev()), future)
	}

	return future
}

func (c *DefaultHandlerContext) Connect(localAddr net.Addr, remoteAddr net.Addr, future Future) Future {
	future = c.checkFuture(future)
	if c.prev() != nil {
		defer c.prev().deferErrorCaught()
		c.prev().handler().Connect(_NewWrapHandlerContext(c._Context(), c.prev()), localAddr, remoteAddr, future)
	}

	return future
}

func (c *DefaultHandlerContext) Disconnect(future Future) Future {
	future = c.checkFuture(future)
	if c.prev() != nil {
		defer c.prev().deferErrorCaught()
		c.prev().handler().Disconnect(_NewWrapHandlerContext(c._Context(), c.prev()), future)
	}

	return future
}

func (c *DefaultHandlerContext) Deregister(future Future) Future {
	future = c.checkFuture(future)
	if c.next() != nil {
		defer c.next().deferErrorCaught()
		c.next().handler().Deregister(_NewWrapHandlerContext(c._Context(), c.next()), future)
	}

	return future
}

func (c *DefaultHandlerContext) prev() HandlerContext {
	return c.prevCtx
}

func (c *DefaultHandlerContext) deferErrorCaught() {
	if v := recover(); v != nil {
		caught := kkpanic.Convert(v)
		kklogger.ErrorJ("HandlerContext.ErrorCaught", caught.Error())
		c.handler().ErrorCaught(c, caught)
	}
}

func (c *DefaultHandlerContext) checkFuture(future Future) Future {
	if future == nil {
		future = c.Channel().Pipeline().NewFuture()
	}

	return future
}

type LogStruct struct {
	Action  string
	Handler string
}
