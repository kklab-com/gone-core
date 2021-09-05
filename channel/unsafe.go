package channel

import (
	"fmt"
	"net"
	"sync/atomic"

	"github.com/kklab-com/goth-kklogger"
	"github.com/kklab-com/goth-kkutil/concurrent"
)

type Unsafe interface {
	Read()
	Write(obj interface{}, future Future)
	Bind(localAddr net.Addr, future Future)
	Close(future Future)
	Connect(localAddr net.Addr, remoteAddr net.Addr, future Future)
	Disconnect(future Future)
}

type DefaultUnsafe struct {
	channel Channel
	readS,
	writeS,
	bindS,
	closeS,
	connectS,
	disconnectS int32
	writeBuffer concurrent.Queue
}

func NewUnsafe(channel Channel) Unsafe {
	return &DefaultUnsafe{channel: channel}
}

func (u *DefaultUnsafe) Read() {
	if channel, ok := u.channel.(UnsafeRead); ok && u.markState(&u.readS) && u.channel.IsActive() {
		go func(u *DefaultUnsafe) {
			defer u.resetState(&u.readS)
			lastObjRead := false
			for {
				obj, err := channel.UnsafeRead()
				if err != nil && err != ErrSkip {
					break
				}

				if err == ErrSkip {
					if u.channel.IsActive() && lastObjRead {
						lastObjRead = false
						u.channel.FireReadCompleted()
					}
				}

				if obj != nil {
					u.channel.FireRead(obj)
					lastObjRead = true
				}

				if !channel.UnsafeIsAutoRead() {
					break
				}
			}
		}(u)
	}
}

func (u *DefaultUnsafe) Write(obj interface{}, future Future) {
	if future == nil {
		future = u.channel.Pipeline().NewFuture()
	}

	if obj != nil && u.channel.IsActive() {
		u.writeBuffer.Push(&unsafeExecuteElem{obj: obj, future: future})
	} else {
		if obj == nil {
			u.futureSuccess(future)
		} else if !u.channel.IsActive() {
			u.futureCancel(future)
			return
		}
	}

	if channel, ok := u.channel.(UnsafeWrite); ok && u.markState(&u.writeS) {
		go func(u *DefaultUnsafe, uw UnsafeWrite) {
			for u.channel.IsActive() {
				elem := func() *unsafeExecuteElem {
					if v := u.writeBuffer.Pop(); v != nil {
						return v.(*unsafeExecuteElem)
					}

					return nil
				}()

				if elem == nil {
					// pending close
					break
				}

				if err := uw.UnsafeWrite(elem.obj); err != nil {
					u.channel.inactiveChannel()
					u.futureCancel(elem.future)
				} else {
					u.futureSuccess(elem.future)
				}

				continue
			}

			if !u.channel.IsActive() {
				for v := u.writeBuffer.Pop(); v != nil; v = u.writeBuffer.Pop() {
					elem := v.(*unsafeExecuteElem)
					u.futureCancel(elem.future)
				}
			}

			u.resetState(&u.writeS)
			if u.writeBuffer.Len() > 0 {
				u.Write(nil, nil)
			}
		}(u, channel)
	}
}

func (u *DefaultUnsafe) Bind(localAddr net.Addr, future Future) {
	if localAddr == nil {
		u.futureCancel(future)
		return
	}

	if channel, ok := u.channel.(UnsafeBind); ok && u.markState(&u.bindS) && !u.channel.CloseFuture().IsDone() {
		go func(u *DefaultUnsafe, elem *unsafeExecuteElem) {
			defer u.resetState(&u.bindS)
			if err := channel.UnsafeBind(elem.localAddr); err != nil {
				kklogger.WarnJ("DefaultUnsafe.Bind", fmt.Sprintf("channel_id: %s, error: %s", u.channel.ID(), err.Error()))
				u.channel.inactiveChannel()
				u.futureCancel(elem.future)
			} else {
				u.channel.activeChannel()
				if channel, ok := u.channel.(UnsafeAccept); ok {
					go func() {
						for u.channel.IsActive() {
							if child, future := channel.UnsafeAccept(); child == nil {
								if u.channel.IsActive() {
									kklogger.WarnJ("DefaultUnsafe.UnsafeAccept", "nil child")
								}

								u.futureCancel(future)
							} else {
								child.Pipeline().fireRegistered()
								child.activeChannel()
								u.futureSuccess(future)
							}
						}
					}()
				}

				u.futureSuccess(elem.future)
			}
		}(u, &unsafeExecuteElem{localAddr: localAddr, future: future})
	}
}

func (u *DefaultUnsafe) Close(future Future) {
	if channel, ok := u.channel.(UnsafeClose); ok && u.markState(&u.closeS) && !u.channel.CloseFuture().IsDone() {
		go func(u *DefaultUnsafe, elem *unsafeExecuteElem) {
			defer u.resetState(&u.closeS)
			u.channel.inactiveChannel().Sync()
			err := channel.UnsafeClose()
			if err != nil {
				kklogger.WarnJ("DefaultUnsafe.Close", fmt.Sprintf("channel_id: %s, error: %s", u.channel.ID(), err.Error()))
			}

			u.futureSuccess(u.channel.CloseFuture())
			u.futureSuccess(elem.future)
		}(u, &unsafeExecuteElem{future: future})
	}
}

func (u *DefaultUnsafe) Connect(localAddr net.Addr, remoteAddr net.Addr, future Future) {
	if remoteAddr == nil {
		u.futureCancel(future)
		return
	}

	if channel, ok := u.channel.(UnsafeConnect); ok && u.markState(&u.connectS) && !u.channel.CloseFuture().IsDone() {
		go func(u *DefaultUnsafe, elem *unsafeExecuteElem) {
			defer u.resetState(&u.connectS)
			if err := channel.UnsafeConnect(elem.localAddr, elem.remoteAddr); err != nil {
				kklogger.WarnJ("DefaultUnsafe.Connect", fmt.Sprintf("channel_id: %s, error: %s", u.channel.ID(), err.Error()))
				u.channel.inactiveChannel()
				u.futureCancel(elem.future)
			} else {
				u.channel.activeChannel()
				u.futureSuccess(elem.future)
			}
		}(u, &unsafeExecuteElem{localAddr: localAddr, remoteAddr: remoteAddr, future: future})
	}
}

func (u *DefaultUnsafe) Disconnect(future Future) {
	if channel, ok := u.channel.(UnsafeDisconnect); ok && u.markState(&u.disconnectS) && !u.channel.CloseFuture().IsDone() {
		go func(u *DefaultUnsafe, elem *unsafeExecuteElem) {
			defer u.resetState(&u.disconnectS)
			u.channel.inactiveChannel()
			err := channel.UnsafeDisconnect()
			u.channel.release()
			if err != nil {
				kklogger.WarnJ("DefaultUnsafe.Disconnect", fmt.Sprintf("channel_id: %s, error: %s", u.channel.ID(), err.Error()))
			}

			u.futureSuccess(elem.future)
		}(u, &unsafeExecuteElem{future: future})
	}
}

func (u *DefaultUnsafe) markState(state *int32) bool {
	return atomic.CompareAndSwapInt32(state, 0, 1)
}

func (u *DefaultUnsafe) resetState(state *int32) {
	atomic.StoreInt32(state, 0)
}

func (u *DefaultUnsafe) futureCancel(future Future) {
	future.Completable().Cancel()
}

func (u *DefaultUnsafe) futureSuccess(future Future) {
	future.Completable().Complete(u.channel)
}

type unsafeExecuteElem struct {
	obj        interface{}
	localAddr  net.Addr
	remoteAddr net.Addr
	future     Future
}
