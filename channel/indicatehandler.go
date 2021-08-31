package channel

import (
	"fmt"
	"net"
)

type IndicateHandlerInbound struct {
	DefaultHandler
}

func (h *IndicateHandlerInbound) Registered(ctx HandlerContext) {
	println(fmt.Sprintf("%s registered", ctx.Channel().ID()))
	ctx.FireRegistered()
}

func (h *IndicateHandlerInbound) Unregistered(ctx HandlerContext) {
	println(fmt.Sprintf("%s unregistered", ctx.Channel().ID()))
	ctx.FireUnregistered()
}

func (h *IndicateHandlerInbound) Active(ctx HandlerContext) {
	println(fmt.Sprintf("%s active", ctx.Channel().ID()))
	ctx.FireActive()
}

func (h *IndicateHandlerInbound) Inactive(ctx HandlerContext) {
	println(fmt.Sprintf("%s inactive", ctx.Channel().ID()))
	ctx.FireInactive()
}

func (h *IndicateHandlerInbound) Read(ctx HandlerContext, obj interface{}) {
	println(fmt.Sprintf("%s read", ctx.Channel().ID()))
	(ctx).FireRead(obj)
}

func (h *IndicateHandlerInbound) ReadCompleted(ctx HandlerContext) {
	println(fmt.Sprintf("%s read_completed", ctx.Channel().ID()))
	(ctx).FireReadCompleted()
}

func (h *IndicateHandlerInbound) Deregister(ctx HandlerContext, future Future) {
	println(fmt.Sprintf("%s deregister", ctx.Channel().ID()))
	ctx.Deregister(future)
}

type IndicateHandlerOutbound struct {
	DefaultHandler
}

func (h *IndicateHandlerOutbound) Write(ctx HandlerContext, obj interface{}, future Future) {
	println(fmt.Sprintf("%s write", ctx.Channel().ID()))
	(ctx).Write(obj, future)
}

func (h *IndicateHandlerOutbound) Bind(ctx HandlerContext, localAddr net.Addr, future Future) {
	println(fmt.Sprintf("%s bind", ctx.Channel().ID()))
	ctx.Bind(localAddr, future)
}

func (h *IndicateHandlerOutbound) Close(ctx HandlerContext, future Future) {
	println(fmt.Sprintf("%s close", ctx.Channel().ID()))
	ctx.Close(future)
}

func (h *IndicateHandlerOutbound) Connect(ctx HandlerContext, localAddr net.Addr, remoteAddr net.Addr, future Future) {
	println(fmt.Sprintf("%s connect", ctx.Channel().ID()))
	ctx.Connect(localAddr, remoteAddr, future)
}

func (h *IndicateHandlerOutbound) Disconnect(ctx HandlerContext, future Future) {
	ctx.Disconnect(future)
}
