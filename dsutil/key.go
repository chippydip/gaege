package dsutil

import (
	"appengine/datastore"
)

func RootKey(key *datastore.Key) *datastore.Key {
	for parent := key; parent != nil; {
		key = parent
		parent = key.Parent()
	}
	return key
}
