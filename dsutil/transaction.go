package dsutil

import (
	"appengine"
	"appengine/datastore"
)

// According to https://developers.google.com/appengine/docs/go/datastore/
// 		An XG transaction that touches only a single entity group has exactly
// 		the same performance and cost as a single-group, non-XG transaction.
//
// Therefore, we can simplify the RunInTransaction interface by just always
// using a cross-group transation (there are no other options currently).
var defaultOpts = &datastore.TransactionOptions{XG: true}

// RunInTransaction is a wrapper around datastore.RunInTransaction that passes
// a default datastore.TransactionOptions object with XG set to true.
func RunInTransaction(ctx appengine.Context, f func(appengine.Context) error) error {
	return datastore.RunInTransaction(ctx, f, defaultOpts)
}
