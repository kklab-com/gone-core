package channel

import (
	"net"
)

type Handler interface {
	Added(ctx HandlerContext)
	Removed(ctx HandlerContext)
	Registered(ctx HandlerContext)
	Unregistered(ctx HandlerContext)
	Active(ctx HandlerContext)
	Inactive(ctx HandlerContext)
	Read(ctx HandlerContext, obj interface{})
	ReadCompleted(ctx HandlerContext)
	Write(ctx HandlerContext, obj interface{}, future Future)
	Bind(ctx HandlerContext, localAddr net.Addr, future Future)
	Close(ctx HandlerContext, future Future)
	Connect(ctx HandlerContext, localAddr net.Addr, remoteAddr net.Addr, future Future)
	Disconnect(ctx HandlerContext, future Future)
	Deregister(ctx HandlerContext, future Future)
	ErrorCaught(ctx HandlerContext, err error)
}

type DefaultHandler struct {
}

func (h *DefaultHandler) Registered(ctx HandlerContext) {
	ctx.FireRegistered()
}

func (h *DefaultHandler) Unregistered(ctx HandlerContext) {
	ctx.FireUnregistered()
}

func (h *DefaultHandler) Active(ctx HandlerContext) {
	ctx.FireActive()
}

func (h *DefaultHandler) Inactive(ctx HandlerContext) {
	ctx.FireInactive()
}

func (h *DefaultHandler) Added(ctx HandlerContext) {
}

func (h *DefaultHandler) Removed(ctx HandlerContext) {
}

func (h *DefaultHandler) Read(ctx HandlerContext, obj interface{}) {
	ctx.FireRead(obj)
}

func (h *DefaultHandler) ReadCompleted(ctx HandlerContext) {
	ctx.FireReadCompleted()
}

func (h *DefaultHandler) Write(ctx HandlerContext, obj interface{}, future Future) {
	ctx.Write(obj, future)
}

func (h *DefaultHandler) Bind(ctx HandlerContext, localAddr net.Addr, future Future) {
	ctx.Bind(localAddr, future)
}

func (h *DefaultHandler) Close(ctx HandlerContext, future Future) {
	ctx.Close(future)
}

func (h *DefaultHandler) Connect(ctx HandlerContext, localAddr net.Addr, remoteAddr net.Addr, future Future) {
	ctx.Connect(localAddr, remoteAddr, future)
}

func (h *DefaultHandler) Disconnect(ctx HandlerContext, future Future) {
	ctx.Disconnect(future)
}

func (h *DefaultHandler) Deregister(ctx HandlerContext, future Future) {
	ctx.Deregister(future)
}

func (h *DefaultHandler) ErrorCaught(ctx HandlerContext, err error) {
	ctx.FireErrorCaught(err)
}

type readWriteHandler struct {
	DefaultHandler
	r func(ctx HandlerContext, obj interface{})
	w func(ctx HandlerContext, obj interface{}, future Future)
}

func NewRWHandler(r func(ctx HandlerContext, obj interface{}), w func(ctx HandlerContext, obj interface{}, future Future)) Handler {
	return &readWriteHandler{r: r, w: w}
}

func (h *readWriteHandler) Read(ctx HandlerContext, obj interface{}) {
	if h.r != nil {
		h.r(ctx, obj)
	} else {
		ctx.FireRead(obj)
	}
}

func (h *readWriteHandler) Write(ctx HandlerContext, obj interface{}, future Future) {
	if h.w != nil {
		h.w(ctx, obj, future)
	} else {
		ctx.Write(obj, future)
	}
}
