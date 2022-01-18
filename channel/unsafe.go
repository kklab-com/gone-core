package channel

import (
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"github.com/kklab-com/goth-kklogger"
	"github.com/kklab-com/goth-kkutil/concurrent"
)

const DefaultAcceptTimeout = 5000

var ErrLocalAddrIsEmpty = fmt.Errorf("local addr is empty")
var ErrRemoteAddrIsEmpty = fmt.Errorf("remote addr is empty")
var ErrChannelNotActive = fmt.Errorf("channel not active")
var ErrChannelClosed = fmt.Errorf("channel closed")
var ErrAcceptTimeout = fmt.Errorf("accept timeout")

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
	if uf, ok := u.channel.(UnsafeRead); ok && u.markState(&u.readS) && u.channel.IsActive() {
		go func(u *DefaultUnsafe, uf UnsafeRead) {
			defer u.resetState(&u.readS)
			lastObjRead := false
			for {
				if obj, err := uf.UnsafeRead(); err != nil {
					if err == ErrSkip {
						if u.channel.IsActive() && lastObjRead {
							lastObjRead = false
							u.channel.FireReadCompleted()
						}
					} else {
						u.channel.inactiveChannel()
						break
					}
				} else {
					if obj != nil {
						u.channel.FireRead(obj)
						lastObjRead = true
					}
				}

				if !uf.UnsafeIsAutoRead() {
					break
				}
			}
		}(u, uf)
	}
}

func (u *DefaultUnsafe) Write(obj interface{}, future Future) {
	if future == nil {
		future = u.channel.Pipeline().NewFuture()
	}

	if obj != nil && u.channel.IsActive() {
		future.(concurrent.Settable).Set(obj)
		u.writeBuffer.Push(future)
	} else {
		if obj == nil {
			u.futureSuccess(future)
		} else if !u.channel.IsActive() {
			u.futureFail(future, ErrChannelNotActive)
			return
		}
	}

	if uf, ok := u.channel.(UnsafeWrite); ok && u.markState(&u.writeS) {
		go func(u *DefaultUnsafe, uf UnsafeWrite) {
			for u.channel.IsActive() {
				future := func() Future {
					if v := u.writeBuffer.Pop(); v != nil {
						return v.(Future)
					}

					return nil
				}()

				if future == nil {
					// pending close
					break
				}

				if err := uf.UnsafeWrite(future.GetNow()); err != nil {
					u.channel.inactiveChannel()
					u.futureFail(future, err)
				} else {
					u.futureSuccess(future)
				}

				continue
			}

			if !u.channel.IsActive() {
				for v := u.writeBuffer.Pop(); v != nil; v = u.writeBuffer.Pop() {
					future := v.(Future)
					if u.channel.CloseFuture().IsDone() {
						u.futureFail(future, ErrChannelClosed)
					} else {
						u.futureFail(future, ErrChannelNotActive)
					}
				}
			}

			u.resetState(&u.writeS)
			if u.writeBuffer.Len() > 0 {
				u.Write(nil, nil)
			}
		}(u, uf)
	}
}

func (u *DefaultUnsafe) Bind(localAddr net.Addr, future Future) {
	if localAddr == nil {
		u.futureFail(future, ErrLocalAddrIsEmpty)
		return
	}

	if _, ok := u.channel.(UnsafeBind); ok && u.markState(&u.bindS) && !u.channel.CloseFuture().IsDone() {
		go func(u *DefaultUnsafe, localAddr net.Addr, future Future) {
			defer u.resetState(&u.bindS)
			if err := u.channel.(UnsafeBind).UnsafeBind(localAddr); err != nil {
				kklogger.WarnJ("DefaultUnsafe.Bind", fmt.Sprintf("channel_id: %s, error: %s", u.channel.ID(), err.Error()))
				u.channel.inactiveChannel()
				u.futureFail(future, err)
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
								go func(u *DefaultUnsafe, child Channel, future Future) {
									child.Pipeline().fireRegistered()
									child.activeChannel()
									u.futureSuccess(future)
								}(u, child, future)

								go func(u *DefaultUnsafe, child Channel, future Future) {
									<-time.After(time.Duration(GetParamIntDefault(child, ParamAcceptTimeout, DefaultAcceptTimeout)) * time.Millisecond)
									if u.futureFail(future, ErrAcceptTimeout) {
										kklogger.ErrorJ("DefaultUnsafe.UnsafeAccept", future.Error().Error())
										child.inactiveChannel()
									}
								}(u, child, future)
							}
						}
					}()
				}

				u.futureSuccess(future)
			}
		}(u, localAddr, future)
	}
}

func (u *DefaultUnsafe) Close(future Future) {
	if channel, ok := u.channel.(UnsafeClose); ok && u.markState(&u.closeS) && !u.channel.CloseFuture().IsDone() {
		go func(u *DefaultUnsafe, future Future) {
			defer u.resetState(&u.closeS)
			func() concurrent.Future { _, f := u.channel.inactiveChannel(); return f }().Await()
			err := channel.UnsafeClose()
			if err != nil {
				kklogger.WarnJ("DefaultUnsafe.Close", fmt.Sprintf("channel_id: %s, error: %s", u.channel.ID(), err.Error()))
			}

			u.futureSuccess(u.channel.CloseFuture())
			u.futureSuccess(future)
		}(u, future)
	}
}

func (u *DefaultUnsafe) Connect(localAddr net.Addr, remoteAddr net.Addr, future Future) {
	if remoteAddr == nil {
		u.futureFail(future, ErrRemoteAddrIsEmpty)
		return
	}

	if channel, ok := u.channel.(UnsafeConnect); ok && u.markState(&u.connectS) && !u.channel.CloseFuture().IsDone() {
		go func(u *DefaultUnsafe, localAddr net.Addr, remoteAddr net.Addr, future Future) {
			defer u.resetState(&u.connectS)
			if err := channel.UnsafeConnect(localAddr, remoteAddr); err != nil {
				kklogger.WarnJ("DefaultUnsafe.Connect", fmt.Sprintf("channel_id: %s, error: %s", u.channel.ID(), err.Error()))
				u.channel.inactiveChannel()
				u.futureCancel(future)
			} else {
				u.channel.activeChannel()
				u.futureSuccess(future)
			}
		}(u, localAddr, remoteAddr, future)
	}
}

func (u *DefaultUnsafe) Disconnect(future Future) {
	if channel, ok := u.channel.(UnsafeDisconnect); ok && u.markState(&u.disconnectS) && !u.channel.CloseFuture().IsDone() {
		go func(u *DefaultUnsafe, future Future) {
			defer u.resetState(&u.disconnectS)
			u.channel.inactiveChannel()
			err := channel.UnsafeDisconnect()
			if err != nil {
				kklogger.WarnJ("DefaultUnsafe.Disconnect", fmt.Sprintf("channel_id: %s, error: %s", u.channel.ID(), err.Error()))
			}

			u.futureSuccess(future)
		}(u, future)
	}
}

func (u *DefaultUnsafe) markState(state *int32) bool {
	return atomic.CompareAndSwapInt32(state, 0, 1)
}

func (u *DefaultUnsafe) resetState(state *int32) {
	atomic.StoreInt32(state, 0)
}

func (u *DefaultUnsafe) futureSuccess(future Future) bool {
	return future.Completable().Complete(u.channel)
}

func (u *DefaultUnsafe) futureFail(future Future, err error) bool {
	return future.Completable().Fail(err)
}

func (u *DefaultUnsafe) futureCancel(future Future) bool {
	return future.Completable().Cancel()
}
