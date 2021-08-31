package channel

import (
	"fmt"
	"net"

	"github.com/kklab-com/goth-kklogger"
	kkpanic "github.com/kklab-com/goth-panic"
)

type Pipeline interface {
	AddLast(name string, elem Handler) Pipeline
	AddBefore(target string, name string, elem Handler) Pipeline
	RemoveFirst() Pipeline
	Remove(elem Handler) Pipeline
	RemoveByName(name string) Pipeline
	Clear() Pipeline
	Channel() Channel
	Param(key ParamKey) interface{}
	SetParam(key ParamKey, value interface{}) Pipeline
	Params() *Params
	fireRegistered() Pipeline
	fireUnregistered() Pipeline
	fireActive() Pipeline
	fireInactive() Pipeline
	fireRead(obj interface{}) Pipeline
	fireReadCompleted() Pipeline
	fireErrorCaught(err error) Pipeline
	Read() Pipeline
	Write(obj interface{}) Future
	Bind(localAddr net.Addr) Future
	Close() Future
	Connect(localAddr net.Addr, remoteAddr net.Addr) Future
	Disconnect() Future
	Deregister() Future
	NewFuture() Future
}

type PipelineSetChannel interface {
	SetChannel(channel Channel)
}

const PipelineHeadHandlerContextName = "DEFAULT_HEAD_HANDLER_CONTEXT"
const PipelineTailHandlerContextName = "DEFAULT_TAIL_HANDLER_CONTEXT"

type DefaultPipeline struct {
	head    HandlerContext
	tail    HandlerContext
	carrier Params
	channel Channel
}

func _NewDefaultPipeline(channel Channel) Pipeline {
	pipeline := new(DefaultPipeline)
	pipeline.head = pipeline._NewHeadHandlerContext()
	pipeline.tail = pipeline._NewTailHandlerContext()
	pipeline.head.setNext(pipeline.tail)
	pipeline.tail.setPrev(pipeline.head)
	channel.setUnsafe(NewUnsafe(channel))
	pipeline.channel = channel
	return pipeline
}

func (p *DefaultPipeline) _NewHeadHandlerContext() HandlerContext {
	context := new(DefaultHandlerContext)
	context.name = PipelineHeadHandlerContextName
	context._handler = &headHandler{}
	context.pipeline = p
	return context
}

func (p *DefaultPipeline) _NewTailHandlerContext() HandlerContext {
	context := new(DefaultHandlerContext)
	context.name = PipelineTailHandlerContextName
	context._handler = &tailHandler{}
	context.pipeline = p
	return context
}

type headHandler struct {
	DefaultHandler
}

func (h *headHandler) read(ctx HandlerContext) {
	ctx.Channel().unsafe().Read()
}

func (h *headHandler) Write(ctx HandlerContext, obj interface{}, future Future) {
	ctx.Channel().unsafe().Write(obj, future)
}

func (h *headHandler) Bind(ctx HandlerContext, localAddr net.Addr, future Future) {
	ctx.Channel().unsafe().Bind(localAddr, future)
}

func (h *headHandler) Close(ctx HandlerContext, future Future) {
	ctx.Channel().unsafe().Close(future)
}

func (h *headHandler) Connect(ctx HandlerContext, localAddr net.Addr, remoteAddr net.Addr, future Future) {
	ctx.Channel().unsafe().Connect(localAddr, remoteAddr, future)
}

func (h *headHandler) Disconnect(ctx HandlerContext, future Future) {
	ctx.Channel().unsafe().Disconnect(future)
}

func (h *headHandler) futureCancel(future Future) {
	future.Completable().Cancel()
}

func (h *headHandler) futureSuccess(future Future) {
	future.Completable().Complete(nil)
}

func (h *headHandler) ErrorCaught(ctx HandlerContext, err error) {
	var ce kkpanic.Caught
	if e, ok := err.(*kkpanic.CaughtImpl); ok {
		ce = e
	} else {
		ce = kkpanic.Convert(e)
	}

	kklogger.ErrorJ("HeadHandler.ErrorCaught", ce)
}

type tailHandler struct {
	DefaultHandler
}

func (h *tailHandler) Read(ctx HandlerContext, obj interface{}) {
	ctx.FireErrorCaught(fmt.Errorf("message doesn't be catched"))
}

func (h *tailHandler) Deregister(ctx HandlerContext, future Future) {
	ctx.Channel().inactiveChannel()
	ctx.Channel().release()
	future.Completable().Complete(nil)
}

func (p *DefaultPipeline) AddLast(name string, elem Handler) Pipeline {
	final := p.tail
	ctx := NewHandlerContext()
	ctx.pipeline = p
	ctx.name = name
	ctx.setNext(final)
	ctx.setPrev(final.prev())
	ctx.next().setPrev(ctx)
	ctx.prev().setNext(ctx)
	ctx._handler = elem
	ctx._handler.Added(p.head)

	return p
}

func (p *DefaultPipeline) AddBefore(target string, name string, elem Handler) Pipeline {
	targetCtx := p.head
	for targetCtx != nil {
		if targetCtx.Name() == target {
			break
		}

		targetCtx = targetCtx.next()
	}

	if targetCtx == nil {
		return p
	}

	ctx := NewHandlerContext()
	ctx.pipeline = p
	ctx.name = name
	ctx.setNext(targetCtx)
	ctx.setPrev(targetCtx.prev())
	ctx.next().setPrev(ctx)
	ctx.prev().setNext(ctx)
	ctx._handler = elem
	ctx._handler.Added(p.head)
	return p
}

func (p *DefaultPipeline) RemoveFirst() Pipeline {
	final := p.head
	if final.next() == nil {
		return p
	}

	next := final.next()
	if next.next() != nil {
		next.next().setPrev(final)
		final.setNext(next.next())
	}

	next.setNext(nil)
	next.setPrev(nil)
	return p
}

func (p *DefaultPipeline) Remove(elem Handler) Pipeline {
	final := p.head.next()
	for final != nil && final != p.tail {
		if final.handler() == elem {
			final.next().setPrev(final.prev())
			final.prev().setNext(final.next())
			final.handler().Removed(final)
			break
		}

		final = final.next()
	}

	return p
}

func (p *DefaultPipeline) RemoveByName(name string) Pipeline {
	final := p.head.next()
	for final != nil {
		if final.Name() == name &&
			name != PipelineHeadHandlerContextName &&
			name != PipelineTailHandlerContextName {
			final.next().setPrev(final.prev())
			final.prev().setNext(final.next())
			final.handler().Removed(final)
			break
		}

		final = final.next()
	}

	return p
}

func (p *DefaultPipeline) Channel() Channel {
	return p.channel
}

func (p *DefaultPipeline) Clear() Pipeline {
	if next := p.head.next(); next != nil {
		next.setPrev(nil)
	}

	if prev := p.tail.prev(); prev != nil {
		prev.setNext(nil)
	}

	p.head.setNext(p.tail)
	p.tail.setPrev(p.head)
	return p
}

func (p *DefaultPipeline) Param(key ParamKey) interface{} {
	if v, f := p.carrier.Load(key); f {
		return v
	}

	return nil
}

func (p *DefaultPipeline) SetParam(key ParamKey, value interface{}) Pipeline {
	p.carrier.Store(key, value)
	return p
}

func (p *DefaultPipeline) Params() *Params {
	return &p.carrier
}

func (p *DefaultPipeline) fireRegistered() Pipeline {
	p.head.FireRegistered()
	return p
}

func (p *DefaultPipeline) fireUnregistered() Pipeline {
	p.head.FireUnregistered()
	return p
}

func (p *DefaultPipeline) fireActive() Pipeline {
	p.head.FireActive()
	return p
}

func (p *DefaultPipeline) fireInactive() Pipeline {
	p.head.FireInactive()
	return p
}

func (p *DefaultPipeline) fireRead(obj interface{}) Pipeline {
	p.head.FireRead(obj)
	return p
}

func (p *DefaultPipeline) fireReadCompleted() Pipeline {
	p.head.FireReadCompleted()
	return p
}

func (p *DefaultPipeline) fireErrorCaught(err error) Pipeline {
	p.head.FireErrorCaught(err)
	return p
}

func (p *DefaultPipeline) Read() Pipeline {
	p.tail.read()
	return p
}

func (p *DefaultPipeline) Write(obj interface{}) Future {
	return p.tail.Write(obj, p.NewFuture())
}

func (p *DefaultPipeline) Bind(localAddr net.Addr) Future {
	return p.tail.Bind(localAddr, p.NewFuture())
}

func (p *DefaultPipeline) Close() Future {
	return p.tail.Close(p.NewFuture())
}

func (p *DefaultPipeline) Connect(localAddr net.Addr, remoteAddr net.Addr) Future {
	return p.tail.Connect(localAddr, remoteAddr, p.NewFuture())
}

func (p *DefaultPipeline) Disconnect() Future {
	return p.tail.Disconnect(p.NewFuture())
}

func (p *DefaultPipeline) Deregister() Future {
	return p.head.Deregister(p.NewFuture())
}

func (p *DefaultPipeline) NewFuture() Future {
	return NewFuture(p.Channel())
}

func (p *DefaultPipeline) SetChannel(channel Channel) {
	p.channel = channel
	channel.setUnsafe(NewUnsafe(channel))
}
