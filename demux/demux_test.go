package demux

import (
	"errors"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestDemuxEtoE(t *testing.T) {
	// Test our Demux using each of our MutexType(s).
	for _, mt := range []MutexType{NoMutex, Mutex, RWMutex} {
		input := make(chan int, 100)
		getID := func(v int) int {
			return v % 2
		}
		ch0 := make(chan int, 1)
		ch1 := make(chan int, 1)

		demux, err := New(input, getID, func(v int, err error) {}, WithMutex[int, int](mt))
		if err != nil {
			panic(err)
		}

		demux.AddReceiver(0, ch0)
		demux.AddReceiver(1, ch1)

		getPacketNum := func(v int) int {
			return v
		}
		orderOut := make(chan int, 1)
		order := NewInOrder(getPacketNum, orderOut)

		wg := sync.WaitGroup{}

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
			log.Println("doing a demux Close")
			demux.Close()
			log.Println("demux Closed")
		}()

		go func() {
			defer log.Println("demux receive finished")
			for {
				select {
				case v, ok := <-ch0:
					if !ok {
						ch0 = nil
						break
					}
					if v%2 != 0 {
						log.Fatalf("TestDemuxEtoE(%v)(channel 0): value %v routed to wrong channel", mt, v)
					}
					order.Add(v)
				case v, ok := <-ch1:
					if !ok {
						ch1 = nil
						break
					}
					if v%2 != 1 {
						log.Fatalf("TestDemuxEtoE(%v)(channel 1): value %v routed to wrong channel", mt, v)
					}
					order.Add(v)
				}
				if ch0 == nil && ch1 == nil {
					log.Println("order.Close() called")
					order.Close()
					return
				}
			}
		}()

		lastVal := -1
		for orderedVal := range orderOut {
			if orderedVal != lastVal+1 {
				t.Fatalf("TestDemuxEtoE(%v): got value %d, wanted %d", mt, orderedVal, lastVal+1)
			}
			lastVal++
		}
	}
}

type valCloseCher struct {
	channel int
	value   int

	close bool
}

func (v valCloseCher) CloseCh() bool {
	return v.close
}

func TestDemuxEtoEWithCloseCher(t *testing.T) {
	input := make(chan valCloseCher, 10)
	getID := func(v valCloseCher) int {
		return v.channel
	}
	ch0 := make(chan valCloseCher, 1)
	ch1 := make(chan valCloseCher, 1)

	demux, err := New(input, getID, func(v valCloseCher, err error) {})
	if err != nil {
		panic(err)
	}

	demux.AddReceiver(0, ch0)
	demux.AddReceiver(1, ch1)

	getPacketNum := func(v valCloseCher) int {
		return v.value
	}

	orderOut := make(chan valCloseCher, 1)
	order := NewInOrder(getPacketNum, orderOut)

	// Send all of our values into the demuxer for input.
	// These must be input in order because our type implements CloseCher().
	go func() {
		for i := 0; i < 10; i++ {
			i := i

			var x valCloseCher
			switch i % 2 {
			case 0:
				x = valCloseCher{channel: 0, value: i}
			case 1:
				x = valCloseCher{channel: 1, value: i}
			}
			if i == 8 || i == 9 {
				x.close = true
			}
			input <- x
		}
	}()

	go func() {
		defer log.Println("demux receive finished")
		for {
			select {
			case v, ok := <-ch0:
				if !ok {
					ch0 = nil
					break
				}
				if v.value%2 != 0 {
					log.Fatalf("TestDemuxEtoEWithCloseCher(channel 0): value %v routed to wrong channel", v)
				}
				log.Printf("%v added to order", v)
				order.Add(v)
			case v, ok := <-ch1:
				if !ok {
					ch1 = nil
					break
				}
				if v.value%2 != 1 {
					log.Fatalf("TestDemuxEtoEWithCloseCher(channel 1): value %v routed to wrong channel", v)
				}
				log.Printf("%v added to order", v)
				order.Add(v)
			}
			if ch0 == nil && ch1 == nil {
				log.Println("order.Close() called")
				order.Close()
				log.Println("order.Close() done")
				return
			}
		}
	}()

	lastVal := -1
	for orderedVal := range orderOut {
		if orderedVal.value != lastVal+1 {
			t.Fatalf("TestDemuxEtoEWithCloseCher: got value %d, wanted %d", orderedVal.value, lastVal+1)
		}
		lastVal++
		log.Println("got: ", orderedVal.value)
	}
}

func TestValClosCher(t *testing.T) {
	demux := &Demux[int, valCloseCher]{}
	v := valCloseCher{close: true}
	if !demux.shouldCloseOnSend(v) {
		t.Fatalf("TestValClosCher: didn't work")
	}
}

func TestCloseOnSendWithoutCloser(t *testing.T) {
	tests := []struct {
		desc         string
		removeOnSend bool
		want         bool
	}{
		{desc: "removeOnSend == true", removeOnSend: true, want: true},
		{desc: "removeOnSend == false", removeOnSend: false, want: false},
	}

	for _, test := range tests {
		demux := &Demux[int, string]{removeOnSend: test.removeOnSend}
		got := demux.shouldCloseOnSend("hello")
		if got != test.want {
			t.Errorf("TestCloseOnSendWithCloser(%s): got %v, want %v", test.desc, got, test.want)
		}
	}
}

type closer string

func (c closer) CloseCh() bool {
	return c == "world"
}

func TestProcess(t *testing.T) {
	type value struct {
		chanID int
		v      string
		err    error
	}

	errHandler := func(v *value, e error) {
		v.err = e
	}

	demux := &Demux[int, *value]{
		getID: func(v *value) int {
			return v.chanID
		},
		delayMax: 10 * time.Millisecond,
		middleware: []Middleware[int, *value]{
			func(v *value) error {
				if v.v == "error" {
					return errors.New("middleware error")
				}
				return nil
			},
		},
		errHandler: errHandler,
	}
	m := map[int]*forwarder[*value]{
		1: {ch: make(chan *value)},
		2: {ch: make(chan *value, 2)},
	}
	demux.mapping.Store(&m)

	tests := []struct {
		desc           string
		val            *value
		demux          *Demux[int, *value]
		wantErrType    ErrType
		wantChanClosed bool
	}{
		{
			desc:        "Middleware has error",
			val:         &value{chanID: 1, v: "error"},
			demux:       demux,
			wantErrType: ETMiddleware,
		},
		{
			desc:        "DelayMax exceeded",
			val:         &value{chanID: 1},
			demux:       demux,
			wantErrType: ETChanTimeout,
		},
		{
			desc:        "Succeeds, but doesn't close channel",
			val:         &value{chanID: 2},
			demux:       demux,
			wantErrType: ETUnknown,
		},
		{
			desc:           "Succeeds, and closes channel",
			val:            &value{chanID: 2},
			demux:          demux,
			wantErrType:    ETUnknown,
			wantChanClosed: true,
		},
	}

	for _, test := range tests {
		test.demux.removeOnSend = test.wantChanClosed

		f := (*test.demux.mapping.Load())[test.val.chanID]
		f.wg.Add(1) // Normally done in loop() before calling.

		if closed := test.demux.process(test.val, test.val.chanID, f); !closed {
			f.wg.Done() // normally done in loop()
		}
		if test.wantErrType != ETUnknown {
			if test.val.err == nil {
				t.Errorf("TestProcess(%s): got err == nil, want err of type == %v", test.desc, test.wantErrType)
			}
			if test.wantErrType != test.val.err.(Error).Type {
				t.Errorf("TestProcess(%s): got err == %v, want err of type == %v", test.desc, test.val.err.(Error).Type, test.wantErrType)
			}
			continue
		}
		if test.val.err != nil {
			t.Errorf("TestProcess(%s): got err == %s, want err == nil", test.desc, test.val.err)
			continue
		}

		if test.wantChanClosed {
			if _, ok := (*demux.mapping.Load())[test.val.chanID]; ok {
				t.Errorf("TestProcess(%s): channel was not closed", test.desc)
			}
		} else {
			select {
			case (*demux.mapping.Load())[test.val.chanID].ch <- test.val:
				<-(*demux.mapping.Load())[test.val.chanID].ch
			default:
				t.Errorf("TestProcess(%s): channel was closed", test.desc)
			}
		}
	}
}

func TestCloseOnSendWithCloser(t *testing.T) {
	tests := []struct {
		desc         string
		v            closer
		removeOnSend bool
		want         bool
	}{
		{desc: "removeOnSend == true", v: closer("hello"), removeOnSend: true, want: true},
		{desc: "removeOnSend == false", v: closer("hello"), removeOnSend: false, want: false},
		{desc: "closer says don't close", v: closer("hello"), removeOnSend: false, want: false},
		{desc: "closer says close", v: closer("world"), removeOnSend: false, want: true},
	}

	for _, test := range tests {
		demux := &Demux[int, closer]{removeOnSend: test.removeOnSend}
		got := demux.shouldCloseOnSend(test.v)
		if got != test.want {
			t.Errorf("TestCloseOnSendWithCloser(%s): got %v, want %v", test.desc, got, test.want)
		}
	}
}

func TestInOrder(t *testing.T) {
	rand.Seed(time.Now().Unix())

	getIDer := func(u uint64) uint64 {
		return u
	}
	out := make(chan uint64, 1)

	order := NewInOrder(getIDer, out)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10000; i++ {
			i := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
				if err := order.Add(uint64(i)); err != nil {
					panic(err)
				}
			}()
		}
	}()

	go func() {
		wg.Wait()
		order.Close()
	}()

	expect := 0
	for v := range out {
		log.Println("got: ", v)
		if v != uint64(expect) {
			t.Fatalf("TestInOrder: received %d, expected %d", v, expect)
		}
		expect++
	}
}
