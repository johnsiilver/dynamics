/*
Package receive provides a stream demuxer for handling results
where you need to send results to different channels based on some identity.

Here is an example:

	// This is uses to send int data to our demuxer. By making the
	// channel hold 100 values, 100 goroutines will be used to do demuxing.
	// This also means that no order is guranteed on the output to the
	// different channels we forward to. If you need guaranteed ordering,
	// you can use the InOrder type. See the demux_test.go file's
	// TestDemuxEtoE() for how to integrate it. However, unless doing complex
	// middleware or the need to prevent head of line blocking for the receiver,
	// a value of 1 will often do.
	input := make(chan int, 100)

	// Here is a function to handle any stream errors. It receives the
	// value "v" and the error. All errors are of type Error, so you
	// can make decisions on what to do based on the error type.
	// This example does nothing.
	errHandle := func(v int, err error){}

	// getID returns a channel ID based on the value modulus 2.
	// So it will always return 0 or 1. In more complex forwarding, the
	// value "v" could simply hold a value "channelID" that the function could
	// return.
	getID := func(v int) int {
		return v % 2
	}

	// Sets up our demuxer that receives data on input. getID() will decide
	// what channel to forward on. If there is an error, it is handled by
	// errHandle.
	demux, err := New(input, getID, errHandle)
	if err != nil {
		panic(err)
	}

	// Here is channels we will forward to.
	output0 := make(chan int, 1)
	output1 := make(chan int, 1)

	// Add our channel receivers. This can be done anytime if you have
	// the need for dynamic handling for new channels. There is also
	// a Remove() to remove a receiver. Also, receivers can be closed
	// when they receive a single value or if the values implement the
	// CloseCher type, the message can signal to close the channel.
	demux.AddReceiver(0, output0)
	demux.AddReceiver(1, output1)

	// Send all of our values into the demuxer for input.
	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < 10000; i++ {
			i := i

			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
				input <- i
			}()
		}
	}()

	go func() {
		wg.Wait()
		demux.Close()
	}()

	dataProcessed := sync.WaitGroup{}
	dataProcessed.Add(1)
	go func() {
		defer dataProcessed.Done()
		for v := range output0 {
			fmt.Println(v)
		}
	}()
	go func() {
		defer dataProcessed.Done()
		for v := range output1 {
			fmt.Println(v)
		}
	}()
	wg.Wait()

For a more complex example that handles reordering messages, see the
demux_test.go file's TestDemuxEtoE().

You can also apply options that require sending on a channel to happen in
some time.Duration or error (prevents head of line blocking).

We support middleware via WithMiddleware().

And if ordering of output matters for particular output but you want
concurrency, you can use the InOrder type to get the values out of the Demuxer
and back into order.

Finally, we support a types that implement CloseCher. If we receive a message
where CloseCh() == true, we forward that message and close the channel. In
this case, all messages going to that channel must be received in order with
the one indicating CloseCh() received last. Otherwise, there is a deadlock.
*/
package demux

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/johnsiilver/pools/goroutines/pooled"

	"github.com/google/btree"
	"golang.org/x/exp/constraints"
)

//go:generate stringer -type=ErrType

// ErrType is the type of error that is being returned.
type ErrType uint8

const (
	ETUnknown ErrType = iota
	// ETValueExists indicates you are creating an exit channel
	// that already exists.
	ETChanExists
	// ETChanNotFound indicates that an exit channel for the value could
	// not be found.
	ETChanNotFound
	// ETMiddleware indicates a piece of middleware gave an error. The
	// error given is wrapped in this error.
	ETMiddleware
	// ETChanTimeout indicates WithDelayMax was set and a value exceeded
	// this delay and is dropped.
	ETChanTimeout
)

// Error provides errors for this module.
type Error struct {
	// Type is the type of error.
	Type ErrType
	// Message is the errors message.
	Message string

	wrapped error
}

func (e Error) Unwrap() error {
	return e.wrapped
}

func (e Error) Wrap(err error) Error {
	e.wrapped = err
	return e
}

func (e Error) Error() string {
	return e.Message
}

func errorf(t ErrType, s string, i ...any) Error {
	return Error{Type: t, Message: fmt.Sprintf(s, i...)}
}

// GetIDer represents a function that handed value V will return the
// unique ID of K.
type GetIDer[K comparable, V any] func(v V) K

// ErrHandler is a function that determines what to do when value V
// was received which generated error err. All errors for processing
// are of our Error type and can be used to decide what to do with
// the value.
type ErrHandler[K comparable, V any] func(v V, err error)

// CloseCher is checked against every input we receive. If the input
// has this method and it returns true, we push the type onto the
// output channel and then close the channel.
type CloseCher interface {
	// CloseCh indicates if this should close the channel the input
	// is routed to. Remember, if the input channel has more than 1
	// in capacity, order cannot be guaranteed and you could be closin
	// a channel that is still processing.
	CloseCh() bool
}

type forwarder[V any] struct {
	wg sync.WaitGroup
	ch chan V
}

// Demux is used to unmux data received on a stream and put it
// on the proper receive channel. K represents a key that can be
// used to identify the channel, V represents the channel value that
// will be sent.
type Demux[K comparable, V any] struct {
	// mutexType indicates what type of Mutex to use.
	mutexType MutexType
	mLock     sync.Mutex
	mRWLock   sync.RWMutex
	mapping   atomic.Pointer[map[K]*forwarder[V]]

	goPool *pooled.Pool

	forwardPool sync.Pool

	addLock sync.Mutex

	// removeOnSend indicates to close the channel we forward to
	// after we receive.
	removeOnSend bool

	input      chan V
	getID      GetIDer[K, V]
	errHandler func(v V, e error)
	delayMax   time.Duration
	middleware []Middleware[K, V]

	// wg tracks how many values are left for processing to prevent
	// Close() from closing while we are still handlling values.
	wg sync.WaitGroup
}

// Option provide optional arguments to New().
type Option[K comparable, V any] func(s *Demux[K, V])

// WithDelayMax tells Stream to call errHandler for a value V
// that is received but can't be router to the destination after
// d time. This prevents head of line blocking.
func WithDelayMax[K comparable, V any](d time.Duration) Option[K, V] {
	return func(s *Demux[K, V]) {
		s.delayMax = d
	}
}

// WithCloseChan says to close the found channel after putting a value on it.
// Useful for when the return channel represents a promise that gets a single
// value.
func WithCloseChan[K comparable, V any]() Option[K, V] {
	return func(s *Demux[K, V]) {
		s.removeOnSend = true
	}
}

//go:generate stringer -type=MutexType

// MutexType indicates the type of mutex to protect the internal datastore with.
type MutexType uint8

const (
	// NoMutex, which is the default, says don't use a mutex. Instead we do a
	// copy of the internal data into a new map every time AddReceiver() is called.
	// The new map is stored in an atomic.Pointer. This is the fastest for reading, but
	// the slowest for writing. This is good when reads vastly outstrip writes
	// and the number of internal channels is low.
	NoMutex MutexType = iota
	// Mutex indicates to use the standard sync.Mutex. Good for when there
	// are symetrical channel adds to responses. So if you are doing a promise
	// type of response (waiting for a single response and then close the forwarding
	// channel), then this is the best option.
	Mutex
	// RWMutex indicates to use the sync.RWMutex. Good when we add
	// new channels much less than we forward to those channels.
	// If the number of forwarding channels is low, NoMutex is probaly the best option.
	RWMutex
)

// WithMutex indicates to use an RWMutex with the Stream.  Without this,
// syncronization occurs by having an internal map copied every time
// an AddReceiver() is called adding the new value and storing it in an
// atomic.Pointer. Slow on write, but fast on reads.  If
func WithMutex[K comparable, V any](mt MutexType) Option[K, V] {
	return func(s *Demux[K, V]) {
		s.mutexType = mt
	}
}

// Middleware is a function that is executed on value v before it is routed.
// If error != nil, further Middleware is not executed and the ErrHandler is
// called for value v.
type Middleware[K comparable, V any] func(v V) error

// WithMiddleware appends Middleware to be executed by the Stream.
func WithMiddleware[K comparable, V any](m ...Middleware[K, V]) Option[K, V] {
	return func(s *Demux[K, V]) {
		s.middleware = append(s.middleware, m...)
	}
}

// New creates a new Demux. in is the channel that this Demux
// will receive values on. The len(in) is equal to the number of goroutines
// that will be used to process incoming values. If len(in) == 0, 1 goroutine
// is used. Concurrent goroutines means that order is not guaranteed.
// getID extracts an ID of type K from a value V.
// ID should be unique to that value V. ID is used to map a received V
// to a channel K. errHandle provides a function that handles what to
// do with an error for value V received on input.
// IMPORTANT: Your input must be in ID order if implementing a value type
// with CloseCher().  Otherwise, this will likely just lock up.
func New[K comparable, V any](in chan V, getID GetIDer[K, V], errHandle ErrHandler[K, V], options ...Option[K, V]) (*Demux[K, V], error) {
	if in == nil || getID == nil || errHandle == nil {
		return nil, fmt.Errorf("in, getID and errHandle cannot be nil")
	}

	s := &Demux[K, V]{
		input:      in,
		getID:      getID,
		errHandler: errHandle,
		forwardPool: sync.Pool{
			New: func() any {
				return &forwarder[V]{}
			},
		},
	}
	m := map[K]*forwarder[V]{}
	s.mapping.Store(&m)
	for _, o := range options {
		o(s)
	}

	l := len(in)
	if l == 0 {
		l = 1
	}
	var err error
	s.goPool, err = pooled.New(l)
	if err != nil {
		panic(err)
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.loop()
	}()

	return s, nil
}

// AddReceiver an ID and a channel to send all values that have that ID onto
// passed channel. This must occur before receiving values of id.
func (s *Demux[K, V]) AddReceiver(id K, ch chan V) error {
	s.addLock.Lock()
	defer s.addLock.Unlock()

	err := s.addMap(id, ch)
	if err != nil {
		return err
	}
	return nil
}

func (s *Demux[K, V]) addMap(id K, ch chan V) error {
	switch s.mutexType {
	case Mutex, RWMutex:
		s.addMapNoCopy(id, ch)
		return nil
	}
	return s.addMapCopy(id, ch)
}

func (s *Demux[K, V]) addMapNoCopy(id K, ch chan V) error {
	switch s.mutexType {
	case Mutex:
		s.mLock.Lock()
		defer s.mLock.Unlock()
	case RWMutex:
		s.mRWLock.Lock()
		defer s.mRWLock.Unlock()
	}

	m := s.mapping.Load()
	_, ok := (*m)[id]
	if ok {
		return errorf(ETChanExists, "%v already exists", id)
	}

	f := s.forwardPool.Get().(*forwarder[V])
	f.ch = ch

	(*m)[id] = f
	return nil
}

func (s *Demux[K, V]) addMapCopy(id K, ch chan V) error {
	m := s.mapping.Load()
	_, ok := (*m)[id]
	if ok {
		return errorf(ETChanExists, "%v already exists", id)
	}

	n := map[K]*forwarder[V]{}
	for k, v := range *m {
		n[k] = v
	}
	f := s.forwardPool.Get().(*forwarder[V])
	f.ch = ch

	n[id] = f
	s.mapping.Store(&n)
	return nil
}

// RemoveReceiver removes a receiver.
func (s *Demux[K, V]) RemoveReceiver(id K) {
	s.remove(id)
}

func (s *Demux[K, V]) remove(id K) {
	f, err := s.getValue(id)
	if err != nil {
		return
	}
	f.wg.Wait()
	close(f.ch)
	log.Println("closed id: ", id)

	switch s.mutexType {
	case Mutex, RWMutex:
		s.removeMapNoCopy(id)
		return
	}
	s.removeMapCopy(id)
}

func (s *Demux[K, V]) removeMapNoCopy(id K) {
	switch s.mutexType {
	case Mutex:
		s.mLock.Lock()
		defer s.mLock.Unlock()
	case RWMutex:
		s.mRWLock.Lock()
		defer s.mRWLock.Unlock()
	}

	m := s.mapping.Load()
	if v, ok := (*m)[id]; ok {
		s.forwardPool.Put(v)
	}
	delete(*m, id)
}

func (s *Demux[K, V]) removeMapCopy(id K) {
	m := s.mapping.Load()
	_, ok := (*m)[id]
	if !ok {
		return
	}

	n := map[K]*forwarder[V]{}
	for k, v := range *m {
		if k == id {
			s.forwardPool.Put(v)
			continue
		}
		n[k] = v
	}
	s.mapping.Store(&n)
}

func (s *Demux[K, V]) getValue(id K) (*forwarder[V], error) {
	switch s.mutexType {
	case Mutex:
		s.mLock.Lock()
		defer s.mLock.Unlock()
	case RWMutex:
		s.mRWLock.RLock()
		defer s.mRWLock.RUnlock()
	}
	v, ok := (*s.mapping.Load())[id]
	if !ok {
		return nil, errorf(ETChanNotFound, "channel for id(%v) was not found", id)
	}
	return v, nil
}

// Close closes our Demux and the input channel it takes in. If there are
// any open outpu channels, they are closed. This will block if until all
// routing goroutines are closed.
func (s *Demux[K, V]) Close() error {
	defer log.Println("Demux Close()")
	close(s.input)
	s.wg.Wait()

	m := s.mapping.Load()

	for _, v := range *m {
		v.wg.Wait()
		close(v.ch)
	}
	return nil
}

func (s *Demux[K, V]) loop() {
	loopWG := sync.WaitGroup{}
	for v := range s.input {
		v := v
		id := s.getID(v)
		f, err := s.getValue(id)
		if err != nil {
			s.errHandler(v, err)
			continue
		}
		loopWG.Add(1)

		f.wg.Add(1) // decrement in process().
		s.goPool.Submit(
			context.Background(),
			func(ctx context.Context) {
				defer loopWG.Done()

				// If we closed the channel, we decrement f.wg beforehand,
				// otherwise .remove() can't wait for all callers to be done.
				if closed := s.process(v, id, f); !closed {
					f.wg.Done()
				}
			},
		)
	}
	loopWG.Wait()
}

func (s *Demux[K, V]) process(v V, id K, forward *forwarder[V]) (closed bool) {
	for _, m := range s.middleware {
		if err := m(v); err != nil {
			err = errorf(ETMiddleware, "middleware had an error").Wrap(err)
			s.errHandler(v, err)
			return closed
		}
	}

	if s.delayMax > 0 {
		t := time.NewTicker(s.delayMax)
		select {
		case forward.ch <- v:
			if s.shouldCloseOnSend(v) {
				closed = true
				s.doCloseOnSend(v, id, forward)
			}
		case <-t.C:
			s.errHandler(v, errorf(ETChanTimeout, "channel(%v) blocked", id))
		}
		return closed
	}
	forward.ch <- v
	if s.shouldCloseOnSend(v) {
		closed = true
		s.doCloseOnSend(v, id, forward)
	}
	return closed
}

func (s *Demux[K, V]) doCloseOnSend(v V, id K, f *forwarder[V]) {
	log.Println("got close on send")
	// Because this is closing, we decrement ourselves before
	// we do the removal so that .remove() can wait for any
	// others process() instances to close.
	f.wg.Done()
	log.Println("doing a remove on ", id)
	s.remove(id)
}

func (s *Demux[K, V]) shouldCloseOnSend(v V) bool {
	if s.removeOnSend {
		return true
	}
	// See if the value implemented CloseCher, if so call it.
	return implementCloseCher(v)
}

func implementCloseCher[V any](v V) bool {
	x, ok := any(v).(CloseCher)
	if !ok {
		return false
	}
	return x.CloseCh()
}

// InOrder allows you to funnel the Demux channel output that will receive
// out of order messages into InOrder which will reprocess the messages
// and send them out in order. InOrder must always receive a value of
// 0 to start processing and expects messages to ascend by 1 from there.
type InOrder[I constraints.Integer, V any] struct {
	mu      sync.Mutex
	counter I
	tree    *btree.BTreeG[V]
	getID   GetIDer[I, V]

	out chan V
}

// NewInOrder makes a new InOrder processor. getID is a GetIDer that gets
// the message's order ID, which should start at 0 and increment.
func NewInOrder[I constraints.Integer, V any](getID GetIDer[I, V], out chan V) *InOrder[I, V] {
	if getID == nil {
		panic("getID cannot be nil")
	}
	if out == nil {
		panic("out cannot be nil")
	}

	n := &InOrder[I, V]{getID: getID, out: out}

	n.tree = btree.NewG(2, n.less)
	return n
}

// Close closes InOrder and closes the out channel.
func (n *InOrder[I, V]) Close() {
	defer log.Println("order.Close() done")
	close(n.out)
	n.tree.Clear(false)
}

// Len returns the length of the internal queue.
func (n *InOrder[I, V]) Len() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.tree.Len()
}

func (n *InOrder[I, V]) less(a, b V) bool {
	return n.getID(a) < n.getID(b)
}

// Add adds a value to be sorted. If the id of v is <= to the current internal counter,
// this will return an error. Nothing is processed until a value with id 0
// is passed, then 1, then 2.
func (n *InOrder[I, V]) Add(v V) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	id := n.getID(v)
	if id < n.counter {
		return fmt.Errorf("current counter is %d, you are adding counter %d, error", n.counter, id)
	}

	if id == n.counter {
		delete := make([]V, 0, n.tree.Len())
		n.out <- v
		n.counter++
		if n.tree.Len() > 0 {
			n.tree.AscendGreaterOrEqual(
				v,
				func(item V) bool {
					if n.getID(item) == n.counter {
						n.out <- item
						delete = append(delete, item)
						n.counter++
						return true
					}
					return false
				},
			)
		}
		if len(delete) == n.tree.Len() {
			n.tree.Clear(false) // This is faster delete path.
		} else {
			for _, d := range delete {
				n.tree.Delete(d)
			}
		}
		return nil
	}
	n.tree.ReplaceOrInsert(v)

	return nil
}
