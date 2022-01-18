package channel

import (
	"fmt"
	"net"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/kklab-com/goth-base62"
	"github.com/kklab-com/goth-kkutil/concurrent"
	"github.com/pkg/errors"
)

type Channel interface {
	Serial() uint64
	ID() string
	Init() Channel
	Pipeline() Pipeline
	CloseFuture() Future
	Bind(localAddr net.Addr) Future
	Close() Future
	Connect(localAddr net.Addr, remoteAddr net.Addr) Future
	Disconnect() Future
	Deregister() Future
	Read() Channel
	FireRead(obj interface{}) Channel
	FireReadCompleted() Channel
	Write(obj interface{}) Future
	IsActive() bool
	SetParam(key ParamKey, value interface{})
	Param(key ParamKey) interface{}
	Params() *Params
	Parent() ServerChannel
	LocalAddr() net.Addr
	init(channel Channel)
	unsafe() Unsafe
	setLocalAddr(addr net.Addr)
	activeChannel()
	inactiveChannel() (success bool, future concurrent.Future)
	setPipeline(pipeline Pipeline)
	setUnsafe(unsafe Unsafe)
	setParent(channel ServerChannel)
	setCloseFuture(future Future)
	release()
}

type UnsafeRead interface {
	UnsafeIsAutoRead() bool
	UnsafeRead() (interface{}, error)
}

type UnsafeBind interface {
	UnsafeBind(localAddr net.Addr) error
}

type UnsafeAccept interface {
	UnsafeAccept() (Channel, Future)
}

type UnsafeClose interface {
	UnsafeClose() error
}

type UnsafeWrite interface {
	UnsafeWrite(obj interface{}) error
}

type UnsafeConnect interface {
	UnsafeConnect(localAddr net.Addr, remoteAddr net.Addr) error
}

type UnsafeDisconnect interface {
	UnsafeDisconnect() error
}

var ErrNotActive = errors.Errorf("channel not active")
var ErrNilObject = fmt.Errorf("nil object")
var ErrUnknownObjectType = fmt.Errorf("unknown object type")
var ErrReadError = fmt.Errorf("read error")
var ErrSkip = fmt.Errorf("skip")

var IDEncoder = base62.FlipEncoding
var serialSequence = uint64(0)

type DefaultChannel struct {
	id          string
	serial      uint64
	Name        string
	alive       concurrent.Future
	params      Params
	localAddr   net.Addr
	pipeline    Pipeline
	_unsafe     Unsafe
	parent      ServerChannel
	closeFuture Future
}

func (c *DefaultChannel) Serial() uint64 {
	return c.serial
}

func (c *DefaultChannel) ID() string {
	return c.id
}

func (c *DefaultChannel) Init() Channel {
	return c
}

func (c *DefaultChannel) Pipeline() Pipeline {
	return c.pipeline
}

func (c *DefaultChannel) CloseFuture() Future {
	return c.closeFuture
}

func (c *DefaultChannel) Bind(localAddr net.Addr) Future {
	return c.Pipeline().Bind(localAddr)
}

func (c *DefaultChannel) Close() Future {
	return c.Pipeline().Close()
}

func (c *DefaultChannel) Connect(localAddr net.Addr, remoteAddr net.Addr) Future {
	return c.Pipeline().Connect(localAddr, remoteAddr)
}

func (c *DefaultChannel) Disconnect() Future {
	return c.Pipeline().Disconnect()
}

func (c *DefaultChannel) Deregister() Future {
	return c.Pipeline().Deregister()
}

func (c *DefaultChannel) Read() Channel {
	c.Pipeline().Read()
	return c
}

func (c *DefaultChannel) FireRead(obj interface{}) Channel {
	c.Pipeline().fireRead(obj)
	return c
}

func (c *DefaultChannel) FireReadCompleted() Channel {
	c.Pipeline().fireReadCompleted()
	return c
}

func (c *DefaultChannel) Write(obj interface{}) Future {
	return c.Pipeline().Write(obj)
}

func (c *DefaultChannel) IsActive() bool {
	return c.alive != nil && !c.alive.IsDone()
}

func (c *DefaultChannel) SetParam(key ParamKey, value interface{}) {
	c.params.Store(key, value)
}

func (c *DefaultChannel) Param(key ParamKey) interface{} {
	if v, f := c.params.Load(key); f {
		return v
	}

	return nil
}

func (c *DefaultChannel) Params() *Params {
	return &c.params
}

func (c *DefaultChannel) Parent() ServerChannel {
	return c.parent
}

func (c *DefaultChannel) LocalAddr() net.Addr {
	return c.localAddr
}

func (c *DefaultChannel) init(channel Channel) {
	u := uuid.New()
	c.id = IDEncoder.EncodeToString(u[:])
	c.serial = atomic.AddUint64(&serialSequence, 1)
	c.setPipeline(_NewDefaultPipeline(channel))
	c.setCloseFuture(c.Pipeline().NewFuture())
}

func (c *DefaultChannel) unsafe() Unsafe {
	return c._unsafe
}

func (c *DefaultChannel) setLocalAddr(addr net.Addr) {
	c.localAddr = addr
}

func (c *DefaultChannel) activeChannel() {
	c.alive = concurrent.NewFuture()
	c.Pipeline().fireActive()
	c.Read()
}

func (c *DefaultChannel) inactiveChannel() (success bool, future concurrent.Future) {
	if c.alive.Completable().Complete(c) {
		cu := c
		rf := c.alive.Then(func(parent concurrent.Future) interface{} {
			// if server channel, wait all child channels be closed.
			if sch, ok := cu.Pipeline().Channel().(ServerChannel); ok {
				sch.waitChildren()
			}

			cu.Pipeline().fireInactive()
			cu.Pipeline().fireUnregistered()
			if _, ok := cu.Pipeline().Channel().(ServerChannel); !ok {
				cu.CloseFuture().Completable().Complete(cu)
			}

			cu.release()
			return cu
		})

		return true, rf
	}

	return false, concurrent.NewCompletedFuture(c)
}

func (c *DefaultChannel) setPipeline(pipeline Pipeline) {
	c.pipeline = pipeline
}

func (c *DefaultChannel) setUnsafe(unsafe Unsafe) {
	c._unsafe = unsafe
}

func (c *DefaultChannel) setParent(channel ServerChannel) {
	c.parent = channel
}

func (c *DefaultChannel) setCloseFuture(future Future) {
	c.closeFuture = future
}

func (c *DefaultChannel) release() {
	if c.Parent() != nil {
		c.Parent().releaseChild(c)
	}
}

func (c *DefaultChannel) UnsafeWrite(obj interface{}) error {
	return nil
}

func (c *DefaultChannel) UnsafeIsAutoRead() bool {
	return true
}

func (c *DefaultChannel) UnsafeRead() (interface{}, error) {
	return nil, nil
}

func (c *DefaultChannel) UnsafeDisconnect() error {
	return nil
}

func (c *DefaultChannel) UnsafeConnect(localAddr net.Addr, remoteAddr net.Addr) error {
	return nil
}
