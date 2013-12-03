package dsutil

import (
	"appengine/datastore"
)

type CacheKey struct {
	*datastore.Key
	str string
}

func NewCacheKey(key *datastore.Key) CacheKey {
	return CacheKey{Key: key}
}

func (ck CacheKey) String() string {
	if ck.str == "" {
		ck.str = ck.Key.String()
	}
	return ck.str
}

func (ck CacheKey) SetString(str string) {
	ck.str = str
}
