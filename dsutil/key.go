package dsutil

import (
	"appengine"
	"appengine/datastore"
)

func RootKey(key *datastore.Key) *datastore.Key {
	for parent := key; parent != nil; {
		key = parent
		parent = key.Parent()
	}
	return key
}

func EntityExists(ctx appengine.Context, key *datastore.Key) (bool, error) {
	// Use an Ancestor query to ensure consistency, but match
	// on the actual key we want to check against.
	q := datastore.NewQuery(key.Kind()).
		Ancestor(key).
		Filter("__key__=", key)

	// Count the results of the query (should be 0 or 1)
	n, err := q.Count(ctx)
	return n > 0, err
}
