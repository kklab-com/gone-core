package channel

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/google/uuid"
	"github.com/kklab-com/goth-base62"
	"github.com/kklab-com/goth-kkutil/concurrent"
	"github.com/pkg/errors"
)

type Channel interface {
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
	context() context.Context
	unsafe() Unsafe
	op() *sync.Mutex
	setLocalAddr(addr net.Addr)
	activeChannel()
	inactiveChannel() Future
	closeWaitGroup() *concurrent.BurstWaitGroup
	setPipeline(pipeline Pipeline)
	setUnsafe(unsafe Unsafe)
	setParent(channel ServerChannel)
	setContext(ctx context.Context)
	setContextCancelFunc(cancel context.CancelFunc)
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

type DefaultChannel struct {
	id            string
	Name          string
	opLock        sync.Mutex
	ctx           context.Context
	ctxCancelFunc context.CancelFunc
	params        Params
	localAddr     net.Addr
	active        bool
	pipeline      Pipeline
	_unsafe       Unsafe
	parent        ServerChannel
	closeFuture   Future
	closeWG       concurrent.BurstWaitGroup
}

func (c *DefaultChannel) ID() string {
	if c.id == "" {
		c.opLock.Lock()
		defer c.opLock.Unlock()
		if c.id == "" {
			u := uuid.New()
			c.id = IDEncoder.EncodeToString(u[:])
		}
	}

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
	return c.active
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

func (c *DefaultChannel) context() context.Context {
	return c.ctx
}

func (c *DefaultChannel) unsafe() Unsafe {
	return c._unsafe
}

func (c *DefaultChannel) op() *sync.Mutex {
	return &c.opLock
}

func (c *DefaultChannel) setLocalAddr(addr net.Addr) {
	c.localAddr = addr
}

func (c *DefaultChannel) activeChannel() {
	c.active = true
	c.Pipeline().fireActive()
	c.Read()
	go func(c Channel) {
		<-c.context().Done()
		if c.IsActive() {
			if _, ok := c.Pipeline().Channel().(ServerChannel); !ok {
				c.Disconnect()
			}
		}
	}(c)
}

func (c *DefaultChannel) inactiveChannel() Future {
	doInactive := c.IsActive()
	c.active = false
	c.ctxCancelFunc()
	future := c.Pipeline().NewFuture()
	go func(c *DefaultChannel) {
		c.closeWG.Wait()
		if doInactive {
			c.Pipeline().fireInactive()
			c.Pipeline().fireUnregistered()
			future.Completable().Complete(nil)
			if _, ok := c.Pipeline().Channel().(ServerChannel); !ok {
				c.CloseFuture().Completable().Complete(nil)
			}
		}
	}(c)

	return future
}

func (c *DefaultChannel) closeWaitGroup() *concurrent.BurstWaitGroup {
	return &c.closeWG
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

func (c *DefaultChannel) setContext(ctx context.Context) {
	c.ctx = ctx
}

func (c *DefaultChannel) setContextCancelFunc(cancel context.CancelFunc) {
	c.ctxCancelFunc = cancel
}

func (c *DefaultChannel) setCloseFuture(future Future) {
	c.closeFuture = future
}

func (c *DefaultChannel) release() {
	if c.Parent() != nil {
		c.Parent().closeWaitGroup().Done()
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
