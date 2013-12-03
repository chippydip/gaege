package testing

import (
	"appengine"
	"appengine/aetest"
	"appengine/datastore"
	"appengine/memcache"
	"appengine_internal"
	"reflect"

	pb "appengine_internal/datastore"

	. "launchpad.net/gocheck"
)

// Context is a wrapper for aetest.Context so we can add methods.
type Context struct {
	aetest.Context
	dskeys map[string]struct{}
}

func (ctx *Context) Call(service, method string, in, out appengine_internal.ProtoMessage, opts *appengine_internal.CallOptions) error {
	switch service {
	case "datastore_v3":
		switch req := in.(type) {
		case *pb.DeleteRequest:
			for _, k := range req.Key {
				key := refToKey(ctx.Context, k)
				delete(ctx.dskeys, key.Encode())
			}
		}
	}

	err := ctx.Context.Call(service, method, in, out, opts)

	switch service {
	case "datastore_v3":
		switch res := out.(type) {
		case *pb.PutResponse:
			if ctx.dskeys == nil {
				ctx.dskeys = map[string]struct{}{}
			}
			for _, k := range res.Key {
				key := refToKey(ctx.Context, k)
				ctx.dskeys[key.Encode()] = struct{}{}
			}
		}
	}

	return err
}

func (ctx *Context) allKeys(c *C) []*datastore.Key {
	keys := make([]*datastore.Key, 0, len(ctx.dskeys))
	for k := range ctx.dskeys {
		key, err := datastore.DecodeKey(k)
		c.Assert(err, IsNil)
		keys = append(keys, key)
	}
	return keys
}

func (ctx *Context) DsCount() int {
	return len(ctx.dskeys)
}

func (ctx *Context) McCount() int {
	stats, err := memcache.Stats(ctx)
	if err != nil {
		panic(err)
	}
	return int(stats.Items)
}

func (ctx *Context) Reset(c *C) {
	// Get the keys for all currently stored Entities
	keys := ctx.allKeys(c)

	// Delete all Entities
	err := datastore.DeleteMulti(ctx, keys)
	c.Assert(err, IsNil)

	// Flush memcache
	err = memcache.Flush(ctx)
	c.Assert(err, IsNil)
}

func refToKey(ctx appengine.Context, ref *pb.Reference) (key *datastore.Key) {
	for _, pe := range ref.GetPath().GetElement() {
		key = datastore.NewKey(ctx, pe.GetType(), pe.GetName(), pe.GetId(), key)
	}
	return key
}

// SetUp creates a new aetest.Context.
func (ctx *Context) SetUp(c *C) {
	var err error
	ctx.Context, err = aetest.NewContext(nil)
	c.Assert(err, IsNil)
}

// TearDown closes the current context (if any).
func (ctx *Context) TearDown(c *C) {
	if ctx.Context != nil {
		ctx.Close()
		ctx.Context = nil
	}
}

type keyEqualsChecker struct {
	*CheckerInfo
}

// KeyEquals checks if the given *datastore.Key has the given Kind, StringID, and IntID, and
// if it has a parent or not.
//     c.Check(key, KeyEquals, "kind", "id", 0, false)
var KeyEquals Checker = keyEqualsChecker{
	&CheckerInfo{Name: "KeyEquals", Params: []string{"key", "kind", "stringID", "intID", "hasParent"}},
}

func (checker keyEqualsChecker) Check(params []interface{}, names []string) (result bool, errStr string) {
	if params[0] == nil {
		return false, "Key is nil"
	}
	key, ok := params[0].(*datastore.Key)
	if !ok {
		return false, "Key is not a *datastore.Key"
	}

	// Try to convert params[3] to an int64 in case it is some other numeric type
	typeOf := reflect.TypeOf(params[3])
	if typeOf.ConvertibleTo(typeOfInt64) {
		params[3] = reflect.ValueOf(params[3]).Convert(typeOfInt64).Interface().(int64)
	}

	names = Equals.Info().Params

	result, errStr = Equals.Check([]interface{}{key.Kind(), params[1]}, names)
	if !result {
		return false, "Kind doesn't match"
	}

	result, errStr = Equals.Check([]interface{}{key.StringID(), params[2]}, names)
	if !result {
		return false, "StringID doesn't match"
	}

	result, errStr = Equals.Check([]interface{}{key.IntID(), params[3]}, names)
	if !result {
		return false, "IntID doesn't match"
	}

	result, errStr = Equals.Check([]interface{}{key.Parent() != nil, params[4]}, names)
	if !result {
		return false, "Parent"
	}

	return true, ""
}
