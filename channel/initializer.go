package channel

type DefaultInitializer struct {
	DefaultHandler
	f func(ch Channel)
}

func NewInitializer(f func(ch Channel)) *DefaultInitializer {
	return &DefaultInitializer{f: f}
}

func (i *DefaultInitializer) Added(ctx HandlerContext) {
	i.f(ctx.Channel())
	ctx.Channel().Pipeline().RemoveFirst()
}
