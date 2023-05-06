package multisingleflight

import (
	"bytes"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
)

// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

//// Package singleflight provides a duplicate function call suppression
//// mechanism.
//package singleflight // import "golang.org/x/sync/singleflight"

import (
	"errors"
)

// errGoexit indicates the runtime.Goexit was called in
// the user given function.
var errGoexit = errors.New("runtime.Goexit was called")

// A panicError is an arbitrary value recovered from a panic
// with the stack trace during the execution of given function.
type panicError struct {
	value interface{}
	stack []byte
}

// Error implements error interface.
func (p *panicError) Error() string {
	return fmt.Sprintf("%v\n\n%s", p.value, p.stack)
}

func newPanicError(v interface{}) error {
	stack := debug.Stack()

	// The first line of the stack trace is of the form "goroutine N [status]:"
	// but by the time the panic reaches Do the goroutine may no longer exist
	// and its status will have changed. Trim out the misleading line.
	if line := bytes.IndexByte(stack[:], '\n'); line >= 0 {
		stack = stack[line+1:]
	}
	return &panicError{value: v, stack: stack}
}

// call is an in-flight or completed singleflight.Do call
type call struct {
	wg sync.WaitGroup

	// These fields are written once before the WaitGroup is done
	// and are only read after the WaitGroup is done.
	val interface{}
	err error
	key string

	// These fields are read and written with the singleflight
	// mutex held before the WaitGroup is done, and are read but
	// not written after the WaitGroup is done.
	dups  int
	chans []chan<- map[string]*Result
}

// Group represents a class of work and forms a namespace in
// which units of work can be executed with duplicate suppression.
type Group struct {
	mu sync.Mutex       // protects m
	m  map[string]*call // lazily initialized
}

// Result holds the results of Do, so they can be passed
// on a channel.
type Result struct {
	Val    interface{}
	Err    error
	Shared bool
}

// Do executes and returns the results of the given function, making
// sure that only one execution is in-flight for a given key at a
// time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
// The return value shared indicates whether v was given to multiple callers.
func (g *Group) Do(keys []string, fn func(keys []string) map[string]*Result) map[string]*Result {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}

	//var newCall *call
	callMap := make(map[*call]struct{})
	left := make([]string, 0, len(keys))

	for _, key := range keys {
		if c, ok := g.m[key]; ok {
			c.dups++
			callMap[c] = struct{}{}
		} else {
			newCall := &call{key: key}
			newCall.wg.Add(1)
			g.m[key] = newCall
			left = append(left, key)
			callMap[newCall] = struct{}{}
		}
	}
	g.mu.Unlock()

	resultMap := make(map[string]*Result)
	if len(left) != 0 {
		g.doCall(left, fn)
	}
	for c := range callMap {
		c.wg.Wait()
		if e, ok := c.err.(*panicError); ok {
			panic(e)
		} else if c.err == errGoexit {
			runtime.Goexit()
		}
		resultMap[c.key] = &Result{Val: c.val, Err: c.err, Shared: c.dups > 0}
	}

	//fmt.Println(len(keys), len(resultMap), keys, resultMap)

	return resultMap
}

// DoChan is like Do but returns a channel that will receive the
// results when they are ready.
//
// The returned channel will not be closed.
func (g *Group) DoChan(keys []string, fn func([]string) map[string]*Result) <-chan map[string]*Result {
	ch := make(chan map[string]*Result, 1)
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}

	callMap := make(map[*call]struct{})
	left := make([]string, 0, len(keys))

	for _, key := range keys {
		if c, ok := g.m[key]; ok {
			c.dups++
			callMap[c] = struct{}{}
		} else {
			newCall := &call{key: key}
			newCall.wg.Add(1)
			g.m[key] = newCall
			left = append(left, key)
			callMap[newCall] = struct{}{}
		}
	}
	g.mu.Unlock()

	go func() {
		resultMap := make(map[string]*Result)
		if len(left) != 0 {
			g.doCall(left, fn)
		}
		for c := range callMap {
			c.wg.Wait()
			if e, ok := c.err.(*panicError); ok {
				panic(e)
			} else if c.err == errGoexit {
				runtime.Goexit()
			}
			resultMap[c.key] = &Result{Val: c.val, Err: c.err, Shared: c.dups > 0}
		}
		ch <- resultMap
	}()

	return ch
}

// doCall handles the single call for a key.
func (g *Group) doCall(keys []string, fn func(keys []string) map[string]*Result) {
	normalReturn := false
	recovered := false

	// use double-defer to distinguish panic from runtime.Goexit,
	// more details see https://golang.org/cl/134395
	defer func() {
		// the given function invoked runtime.Goexit
		if !normalReturn && !recovered {
			for _, key := range keys {
				g.m[key].err = errGoexit
			}
			//c.err = errGoexit
		}

		g.mu.Lock()
		defer g.mu.Unlock()
		//c.wg.Done()
		errs := make([]error, 0, len(keys))
		for _, key := range keys {
			if _, ok := g.m[key]; !ok {
				continue
			}
			g.m[key].wg.Done()
			errs = append(errs, g.m[key].err)
			delete(g.m, key)
		}

		for _, err := range errs {
			if e, ok := err.(*panicError); ok {
				// In order to prevent the waiting channels from being blocked forever,
				// needs to ensure that this panic cannot be recovered.
				//if len(c.chans) > 0 {
				//	go panic(e)
				//	select {} // Keep this goroutine around so that it will appear in the crash dump.
				//} else {
				//	panic(e)
				//}
				panic(e)
			} else if err == errGoexit {
				// Already in the process of goexit, no need to call again
			} else {
				//// Normal return
				//for _, ch := range c.chans {
				//	ch <- Result{c.val, c.err, c.dups > 0}
				//}
			}
		}

	}()

	func() {
		defer func() {
			if !normalReturn {
				// Ideally, we would wait to take a stack trace until we've determined
				// whether this is a panic or a runtime.Goexit.
				//
				// Unfortunately, the only way we can distinguish the two is to see
				// whether the recover stopped the goroutine from terminating, and by
				// the time we know that, the part of the stack trace relevant to the
				// panic has been discarded.
				if r := recover(); r != nil {
					for _, key := range keys {
						g.m[key].err = newPanicError(r)
					}
				}
			}
		}()

		resultMap := fn(keys)
		g.mu.Lock()
		defer g.mu.Unlock()
		for _, key := range keys {
			c := g.m[key]
			if _, ok := resultMap[key]; !ok {
				continue
			}
			c.val = resultMap[key].Val
			c.err = resultMap[key].Err
		}
		normalReturn = true
	}()

	if !normalReturn {
		recovered = true
	}
}

// Forget tells the singleflight to forget about a key.  Future calls
// to Do for this key will call the function rather than waiting for
// an earlier call to complete.
func (g *Group) Forget(key string) {
	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
}
