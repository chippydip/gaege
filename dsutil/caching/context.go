package caching

import (
	"appengine"
	"appengine_internal"
	"net/http"

	pb "appengine_internal/datastore"
)

type Options struct {
	// TODO
}

func (opt *Options) cacheWrites() bool {
	return false
}

// Context is a wrapper for aetest.Context so we can add methods.
// TODO: add a mutex in case the Context is used in a multi-threaded way?
// TODO: handle concurrent transactions in multiple go routines
type Context struct {
	appengine.Context
	options *Options
	tx      map[string]*pb.GetResponse_Entity
	//cache   map[string]*pb.GetResponse_Entity
}

func NewContext(r *http.Request, opts *Options) *Context {
	return &Context{
		Context: appengine.NewContext(r),
		options: opts,
	}
}

func WrapContext(ctx appengine.Context, opts *Options) *Context {
	return &Context{
		Context: ctx,
		options: opts,
	}
}

func (ctx *Context) IsInTransaction() bool {
	return ctx.tx != nil
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
