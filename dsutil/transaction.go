package dsutil

import (
	"appengine"
	"appengine/datastore"
	"reflect"
)

var defaultOpts = &datastore.TransactionOptions{XG: true}

// RunInTransaction is a wrapper around datastore.RunInTransaction that passes
// a default datastore.TransactionOptions object with XG set to true.
//
// According to https://developers.google.com/appengine/docs/go/datastore/
// 		An XG transaction that touches only a single entity group has exactly
// 		the same performance and cost as a single-group, non-XG transaction.
//
// Therefore, we can simplify the RunInTransaction interface by just always
// using a cross-group transaction (there are no other options currently).
func RunInTransaction(ctx appengine.Context, f func(appengine.Context) error) error {
	return datastore.RunInTransaction(ctx, f, defaultOpts)
}

// IsInTransaction tests if the given transaction was one created by a call
// to datastore.RunInTransaction (or a wrapper like the one above).
//
// NB: If a transaction context is wrapped inside another context object this
// method will not detect it as a transaction. While it may be possible to
// catch this error in some cases via reflection, in general a transaction
// context should not be wrapped. The default datastore.RunInTransaction method
// uses a simple type cast to detect and prevent nested transactions and
// wrapping a transaction context will prevent this check from working.
func IsInTransaction(ctx appengine.Context) bool {
	// TODO: try using RegisterTransactionSetter with a dummy ctx.Call for this?
	// We can't type assert a private type in another package, so fake it.
	return reflect.TypeOf(ctx).String() == "*datastore.transaction"
}
