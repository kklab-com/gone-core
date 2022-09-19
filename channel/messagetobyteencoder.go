package channel

import (
	buf "github.com/kklab-com/goth-bytebuf"
)

type MessageEncoder interface {
	Encode(ctx HandlerContext, msg any, out buf.ByteBuf)
}

type MessageToByteEncoder struct {
	DefaultHandler
	Encode func(ctx HandlerContext, msg any, out buf.ByteBuf)
}

func (h *MessageToByteEncoder) Added(ctx HandlerContext) {
	if h.Encode == nil {
		h.Encode = h.encode
	}
}

func (h *MessageToByteEncoder) Write(ctx HandlerContext, obj any, future Future) {
	out := buf.EmptyByteBuf()
	h.Encode(ctx, obj, out)
	ctx.Write(out, future)
}

func (h *MessageToByteEncoder) encode(ctx HandlerContext, msg any, out buf.ByteBuf) {
	panic("implement me")
}
