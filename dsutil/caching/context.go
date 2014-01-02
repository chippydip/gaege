package caching

import (
	"appengine"
	"appengine_internal"
	"net/http"
	"sync"

	pb "appengine_internal/datastore"
)

type Options struct {
	// TODO
}

func (opt *Options) cacheWrites() bool {
	return false
}

type transactionMap map[string]*pb.GetResponse_Entity
type transactionMaps map[*pb.Transaction]transactionMap

// Context is a wrapper for aetest.Context so we can add methods.
type Context struct {
	appengine.Context
	options *Options

	// Mutable state should be read or written while holding this lock, but
	// it shouldn't be held through API calls.
	mu sync.Mutex
	tx transactionMaps
	//cache   map[string]*pb.GetResponse_Entity
}

func NewContext(r *http.Request, opts *Options) *Context {
	return &Context{
		Context: appengine.NewContext(r),
		options: opts,
		tx:      transactionMaps{},
	}
}

func WrapContext(ctx appengine.Context, opts *Options) *Context {
	return &Context{
		Context: ctx,
		options: opts,
		tx:      transactionMaps{},
	}
}

const (
	kDatastore = "datastore_v3"
	kMemcache  = "memcache"
)

func (ctx *Context) Call(service, method string, in, out appengine_internal.ProtoMessage, opts *appengine_internal.CallOptions) error {
	//ctx.Infof("Call(%q, %q, %v, %v, %v)", service, method, in, out, opts)

	switch service {
	case kDatastore:
		return ctx.datastoreCall(method, in, out, opts)
	case kMemcache:
		return ctx.memcacheCall(method, in, out, opts)
	default:
		return ctx.Context.Call(service, method, in, out, opts)
	}
}

//////////////////////////////////////////////////////////////////////////////

func (ctx *Context) newTransactionMap(tx *pb.Transaction) {
	if tx == nil {
		ctx.Criticalf("caching: can't create <nil> transaction")
		return
	}

	ctx.mu.Lock()
	defer ctx.mu.Unlock()

	if ctx.tx[tx] != nil {
		ctx.Criticalf("caching: transaction already started (%v)", tx)
		return
	}

	ctx.tx[tx] = transactionMap{}
}

func (ctx *Context) updateTransactionMap(tx *pb.Transaction, keys []string, values []*pb.GetResponse_Entity) {
	ctx.mu.Lock()
	defer ctx.mu.Unlock()

	m := ctx.tx[tx]

	// We should be in a transaction
	if m == nil {
		ctx.Criticalf("caching: no transaction for %v", tx)
		return
	}

	// Handle puts/deletes
	if values == nil {
		for _, key := range keys {
			m[key] = nil
		}
		return
	}

	// Handle gets
	for i, key := range keys {
		if i >= len(values) {
			ctx.Criticalf("caching: len(keys) != len(values) (%v, %v)", len(keys), len(values))
			break
		}
		m[key] = values[i]
	}
}

func (ctx *Context) removeTransactionMap(tx *pb.Transaction) transactionMap {
	ctx.mu.Lock()
	ctx.mu.Unlock()

	m := ctx.tx[tx]

	delete(ctx.tx, tx)

	return m
}
