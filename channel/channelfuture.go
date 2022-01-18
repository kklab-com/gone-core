package channel

import (
	"github.com/kklab-com/goth-kkutil/concurrent"
)

type Future interface {
	concurrent.Future
	Sync() Future
	Channel() Channel
	_channel() Channel
}

type DefaultFuture struct {
	concurrent.Future
	channel Channel
}

func NewFuture(channel Channel) Future {
	future := &DefaultFuture{
		Future:  concurrent.NewFuture(),
		channel: channel,
	}

	return future
}

func (d *DefaultFuture) Sync() Future {
	d.Future.Await()
	return d
}

func (d *DefaultFuture) Set(obj interface{}) {
	d.Future.(concurrent.Settable).Set(obj)
}

func (d *DefaultFuture) Channel() Channel {
	if !d.IsDone() {
		return nil
	} else {
		if d.IsSuccess() {
			return d._channel()
		}
	}

	return nil
}

func (d *DefaultFuture) _channel() Channel {
	return d.channel
}
