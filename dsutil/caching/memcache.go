package caching

import (
	"appengine_internal"

	//"code.google.com/p/goprotobuf/proto"

	//pb "appengine_internal/memcache"
)

// 'memcache': {
//     'Get':       (memcache_service_pb.MemcacheGetRequest,
//                   memcache_service_pb.MemcacheGetResponse),
//     'Set':       (memcache_service_pb.MemcacheSetRequest,
//                   memcache_service_pb.MemcacheSetResponse),
//     'Delete':    (memcache_service_pb.MemcacheDeleteRequest,
//                   memcache_service_pb.MemcacheDeleteResponse),
//     'Increment': (memcache_service_pb.MemcacheIncrementRequest,
//                   memcache_service_pb.MemcacheIncrementResponse),
//     'BatchIncrement': (memcache_service_pb.MemcacheBatchIncrementRequest,
//                        memcache_service_pb.MemcacheBatchIncrementResponse),
//     'FlushAll':  (memcache_service_pb.MemcacheFlushRequest,
//                   memcache_service_pb.MemcacheFlushResponse),
//     'Stats':     (memcache_service_pb.MemcacheStatsRequest,
//                   memcache_service_pb.MemcacheStatsResponse),
// },

func (ctx *Context) memcacheCall(method string, in, out appengine_internal.ProtoMessage, opts *appengine_internal.CallOptions) error {
	// TODO: ctx.cache
	return ctx.Context.Call(kMemcache, method, in, out, opts)
}

// // Check the ctx.cache first
// // TODO: make this optional
// for i, key := range keys {
// 	i = indexes[i] // noop for consistency
// 	results[i] = ctx.cache[key]
// }
// keys, indexes = reduce(keys, indexes, results)
