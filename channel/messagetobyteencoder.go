package channel

import (
	"github.com/kklab-com/goth-kkutil/buf"
)

type MessageEncoder interface {
	Encode(ctx HandlerContext, msg interface{}, out buf.ByteBuf)
}

type MessageToByteEncoder struct {
	DefaultHandler
	Encode func(ctx HandlerContext, msg interface{}, out buf.ByteBuf)
}

func (h *MessageToByteEncoder) Added(ctx HandlerContext) {
	if h.Encode == nil {
		h.Encode = h.encode
	}
}

func (h *MessageToByteEncoder) Write(ctx HandlerContext, obj interface{}, future Future) {
	out := buf.EmptyByteBuf()
	h.Encode(ctx, obj, out)
	ctx.Write(out, future)
}

func (h *MessageToByteEncoder) encode(ctx HandlerContext, msg interface{}, out buf.ByteBuf) {
	panic("implement me")
}
