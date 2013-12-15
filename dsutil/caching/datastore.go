package caching

import (
	"appengine/memcache"
	"appengine_internal"

	"code.google.com/p/goprotobuf/proto"

	pb "appengine_internal/datastore"
)

// 'datastore_v3': {
//     'Get':        (datastore_pb.GetRequest, datastore_pb.GetResponse),
//     'Put':        (datastore_pb.PutRequest, datastore_pb.PutResponse),
//     'Delete':     (datastore_pb.DeleteRequest, datastore_pb.DeleteResponse),
//     'AllocateIds':(datastore_pb.AllocateIdsRequest,
//                    datastore_pb.AllocateIdsResponse),
//     'RunQuery':   (datastore_pb.Query,
//                    datastore_pb.QueryResult),
//     'Next':       (datastore_pb.NextRequest, datastore_pb.QueryResult),
//     'BeginTransaction':(datastore_pb.BeginTransactionRequest,
//                         datastore_pb.Transaction),
//     'Commit':          (datastore_pb.Transaction,
//                         datastore_pb.CommitResponse),
//     'Rollback':        (datastore_pb.Transaction,
//                         api_base_pb.VoidProto),
//     'GetIndices':      (api_base_pb.StringProto,
//                         datastore_pb.CompositeIndices),
// },
// 'datastore_v4': {
//     'AllocateIds': (datastore_v4_pb.AllocateIdsRequest,
//                     datastore_v4_pb.AllocateIdsResponse),
// },

func (ctx *Context) datastoreCall(method string, in, out appengine_internal.ProtoMessage, opts *appengine_internal.CallOptions) error {
	// Get requires special handling to try and pull values from cache and request only what's not found
	if method == "Get" && !ctx.IsInTransaction() {
		return ctx.datastoreGet(in.(*pb.GetRequest), out.(*pb.GetResponse), opts)
	}

	// Perform the actual call with the underlying Context
	err := ctx.Context.Call(kDatastore, method, in, out, opts)

	// Update the cache based on the request and response data
	switch method {
	case "Get":
		// Should be in a transaction here (or datastoreGet would have been used above),
		// so remember the results to be added to the cache when the transaction commits
		keys := refsToKeys(in.(*pb.GetRequest).Key)
		values := out.(*pb.GetResponse).Entity
		for i, key := range keys {
			if i >= len(values) {
				break
			}
			ctx.tx[key] = values[i]
		}

	case "Put":
		// TODO: optionally store the put values to save the next lookup
		ctx.datastoreDelete(out.(*pb.PutResponse).Key)

	case "Delete":
		ctx.datastoreDelete(in.(*pb.DeleteRequest).Key)

	case "RunQuery", "Next":
		// TODO: optionally cache results

	case "BeginTransaction":
		// Create a new transaction cache
		ctx.tx = map[string]*pb.GetResponse_Entity{}

	case "Commit":
		// Perform delayed updates
		ctx.datastoreCommit()

	case "Rollback":
		// Discard remembered updates (should have been rolled back)
		ctx.tx = nil
	}

	return err
}

func (ctx *Context) datastoreGet(in *pb.GetRequest, out *pb.GetResponse, opts *appengine_internal.CallOptions) (err error) {
	// return ctx.Context.Call(kDatastore, "Get", in, out, opts)

	// Collect results from different places here
	results := make([]*pb.GetResponse_Entity, len(in.Key))

	// Stringify the requested keys for cache lookups and
	// setup an initial 1-to-1 mapping to the results slice
	keys := refsToKeys(in.Key)
	indexes := make([]int, len(keys))
	for i := 0; i < len(indexes); i++ {
		indexes[i] = i
	}

	// Check memcache for any remaining values
	// TODO: make this optional
	if len(keys) > 0 {
		if items, e := memcache.GetMulti(ctx, keys); e != nil {
			ctx.Warningf("caching: memcache.GetMulti: %v", e)
		} else {
			// Add the results from memcache
			for x, key := range keys {
				i := indexes[x]

				results[i] = ctx.unmarshal(items[key])
			}
			keys, indexes = reduce(keys, indexes, results)
		}
	}

	// Make the actual datastore call for any remaining entities
	if len(keys) > 0 {
		origInKey := in.Key // save the original list of keys to restore later if needed

		// If any values came from caches, don't request them from the datastore
		if len(keys) < len(in.Key) {
			refs := make([]*pb.Reference, len(keys))
			for x, i := range indexes {
				refs[x] = in.Key[i]
			}
			in.Key = refs
		}

		// Make the underlying API call
		err = ctx.Context.Call(kDatastore, "Get", in, out, opts)

		// Un-patch the request (may be a noop)
		in.Key = origInKey

		// Build a list of memcache.Items to add to memcache
		items := make([]*memcache.Item, 0, len(keys))
		for x, value := range out.Entity {
			i, key := indexes[x], keys[x]
			results[i] = value

			if value != nil {
				items = ctx.appendItem(items, key, value)
			}
		}

		// Add the values to memcache
		if e := memcache.SetMulti(ctx, items); e != nil {
			ctx.Warningf("caching: memcache.SetMulti: %v", e)
		}
	}

	// Store the full results and return
	out.Entity = results
	return err
}

func reduce(keys []string, indexes []int, values []*pb.GetResponse_Entity) ([]string, []int) {
	count := 0
	for i, j := range indexes {
		// Keep only key/index pairs that do not yet have a value
		if values[j] == nil {
			// Compact these values at the front of the array
			keys[count] = keys[i]
			indexes[count] = indexes[i]
			count++
		}
	}
	// Return the compacted portion
	return keys[:count], indexes[:count]
}

func (ctx *Context) datastoreDelete(refs []*pb.Reference) {
	keys := refsToKeys(refs)

	if !ctx.IsInTransaction() {
		// clear the cached value
		if e := ignoreMisses(memcache.DeleteMulti(ctx, keys)); e != nil {
			ctx.Warningf("caching: memcache.DeleteMulti: %v", e)
		}
	} else {
		// remember for update once tx commits
		for _, key := range keys {
			ctx.tx[key] = nil
		}
	}
}

func (ctx *Context) datastoreCommit() {
	// Split the remembered updates into puts/deletes and gets
	deletes := make([]string, 0, len(ctx.tx))
	items := make([]*memcache.Item, 0, len(ctx.tx))
	for key, value := range ctx.tx {
		if value == nil {
			deletes = append(deletes, key)
		} else {
			ctx.appendItem(items, key, value)
		}
	}
	ctx.tx = nil

	// Perform the deletes
	if len(deletes) > 0 {
		if e := ignoreMisses(memcache.DeleteMulti(ctx, deletes)); e != nil {
			ctx.Warningf("caching: memcache.DeleteMulti: %v", e)
		}
	}

	// Perform the updates
	if len(items) > 0 {
		if e := memcache.SetMulti(ctx, items); e != nil {
			ctx.Warningf("caching: memcache.SetMulti: %v", e)
		}
	}
}

func ignoreMisses(err error) error {
	if me, ok := err.(appengine.MultiError); ok {
		any = false
		for i, e := range me {
			if e == memcache.ErrCacheMiss {
				me[i] = nil
			} else if e != nil {
				any = true
			}
		}
		if !any {
			err = nil
		}
	}
	return err
}

func refsToKeys(refs []*pb.Reference) []string {
	keys := make([]string, len(refs))
	for i, ref := range refs {
		if ref != nil {
			keys[i] = ref.String()
		}
	}
	return keys
}

func (ctx *Context) appendItem(items []*memcache.Item, key string, value *pb.GetResponse_Entity) []*memcache.Item {
	// Marshal the value so it can be put into memcache
	if buf, e := proto.Marshal(value); e != nil {
		ctx.Errorf("caching: marshalling error: %v", e) // shouldn't happen
	} else {
		items = append(items, &memcache.Item{
			Key:   key,
			Value: buf,
		})
	}
	return items
}

func (ctx *Context) unmarshal(item *memcache.Item) *pb.GetResponse_Entity {
	if item == nil {
		return nil
	}

	value := new(pb.GetResponse_Entity)
	if e := proto.Unmarshal(item.Value, value); e != nil {
		ctx.Warningf("caching: bad value for %v (%v)", item.Key, e)
		return nil
	}
	return value
}
