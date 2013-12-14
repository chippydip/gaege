package unique

import (
	"appengine"
	"appengine/datastore"
	"errors"

	"github.com/chippydip/gaege/dsutil"
)

var ErrDuplicateIndexValue = errors.New("unique: duplicate index value")

type Flag int

const (
	SingleEntityGroup Flag = 1 << iota
	SaveOldValues
	PreventReuse
)

type Index struct {
	name  string
	flags Flag
}

func NewIndex(name string, flags Flag) Index {
	if flags&PreventReuse != 0 {
		// Preventing reuse requires saving old values
		flags |= SaveOldValues
	}
	return Index{
		name:  name,
		flags: flags,
	}
}

const (
	idEntity    = "I"
	valueEntity = "V"
)

func (idx Index) newKey(ctx appengine.Context, kind, id string) (key *datastore.Key) {
	if idx.flags&SingleEntityGroup != 0 {
		key = datastore.NewKey(ctx, idx.name, "", 1, nil)
	}
	kind = idx.name + kind
	return datastore.NewKey(ctx, kind, id, 0, key)
}

func (idx Index) GetValue(ctx appengine.Context, id string) (value string, err error) {
	return get(ctx, idx.newKey(ctx, idEntity, id))
}

func (idx Index) GetId(ctx appengine.Context, value string) (id string, err error) {
	key := idx.newKey(ctx, valueEntity, value)
	id, err = get(ctx, key)

	// If old values were supposed to be deleted, make sure this isn't an old value
	if err == nil && idx.flags&SaveOldValues == 0 {

		// Get the canonical version (should be the same as value)
		canonical, err := get(ctx, idx.newKey(ctx, idEntity, id))
		if err != nil {
			return "", err
		}
		if value != canonical {
			// Yikes! This should have been deleted, so try again and return not found
			del(ctx, key)
			return "", datastore.ErrNoSuchEntity
		}
	}

	return id, err
}

func (idx Index) Set(ctx appengine.Context, id, value string) error {
	return dsutil.RunInTransaction(ctx, func(ctx appengine.Context) error {
		valueKey := idx.newKey(ctx, idEntity, id)
		idKey := idx.newKey(ctx, valueEntity, value)

		// Check for an existing key for this value
		currId, err := get(ctx, idKey)
		if err != datastore.ErrNoSuchEntity {
			if err != nil {
				return err
			}
			// value exist

			if currId == id {
				return nil // already set
			}
			// value is mapped to another ID

			if idx.flags&PreventReuse != 0 {
				return ErrDuplicateIndexValue
			}
			// value can be reused

			// Check if value is canonical for currId
			canonical, err := idx.GetValue(ctx, currId)
			if err != nil {
				return err
			}
			if value == canonical {
				return ErrDuplicateIndexValue
			}
			// value is non canonical and can be reused
		}
		// ok to insert/update the value

		// Should we try to delete the old value?
		if idx.flags&SaveOldValues == 0 {
			// Note: failure here is non-fatal since GetId will ignore
			// (and try to delete again) any non-canonical values it may find
			if oldValue, err := idx.GetValue(ctx, id); err == nil {
				key := idx.newKey(ctx, valueEntity, oldValue)
				del(ctx, key)
			}
		}

		// Update the value index and then the id index
		err = put(ctx, idKey, id)
		if err != nil {
			return err
		}
		return put(ctx, valueKey, value)
	})
}

func get(ctx appengine.Context, key *datastore.Key) (prop string, err error) {
	err = datastore.Get(ctx, key, stringPLS{&prop})
	return prop, err
}

func put(ctx appengine.Context, key *datastore.Key, prop string) (err error) {
	_, err = datastore.Put(ctx, key, stringPLS{&prop})
	return err
}

func del(ctx appengine.Context, key *datastore.Key) {
	datastore.Delete(ctx, key)
}

// stringPLS implements datastore.PropertyLoadSaver for a single string
type stringPLS struct{ *string }

const propName = "$"

func (pls stringPLS) Load(c <-chan datastore.Property) error {
	for p := range c {
		if p.Name == propName && pls.string != nil {
			if s, ok := p.Value.(string); ok {
				*pls.string = s
			}
		}
	}

	return nil
}

func (pls stringPLS) Save(c chan<- datastore.Property) error {
	defer close(c)

	c <- datastore.Property{
		Name:    propName,
		Value:   *pls.string,
		NoIndex: true,
	}

	return nil
}
