package testing

import (
	"appengine/datastore"
	"fmt"
	"reflect"

	. "launchpad.net/gocheck"
)

// reflect.TypeOf(int64)
var typeOfInt64 = reflect.TypeOf((*int64)(nil)).Elem()

// Key builds a datastore.Key from pairs of (kind, id) arguments. Kind must
// be a string while id can be any value. If it is convertible to int64 then
// it is used as an IntID, otherwise it is stringified and used as the
// StringID. If more than one pair of values are given, the first becomes the
// parent of the second, and so on. If any errors occur, nil is returned.
func (ctx *Context) Key(args ...interface{}) (key *datastore.Key) {
	// Must have an even number of args
	if len(args)%2 != 0 {
		return nil
	}

	// Loop through the arg pairs
	for i := 0; i < len(args); i += 2 {
		kind, ok := args[i].(string)
		if !ok {
			return nil
		}
		value := args[i+1]

		// Parse the second arg as either a number or a string id
		stringId, intId := "", int64(0)
		typeOf := reflect.TypeOf(value)
		if typeOf.ConvertibleTo(typeOfInt64) {
			intId = reflect.ValueOf(value).Convert(typeOfInt64).Interface().(int64)
		} else {
			stringId = fmt.Sprint(value)
		}

		// Create the key as a child of the previous key (if any)
		key = datastore.NewKey(ctx, kind, stringId, intId, key)
	}

	return key
}

type Entity map[string]interface{}

func (e Entity) Key() *datastore.Key {
	key := e["__key__"]
	if key != nil {
		return key.(*datastore.Key)
	} else {
		return nil
	}
}

func (e Entity) Load(c <-chan datastore.Property) error {
	for p := range c {
		e[p.Name] = p.Value
	}

	return nil
}

func (e Entity) Save(c chan<- datastore.Property) error {
	defer close(c)

	for name, value := range e {
		if name != "__key__" {
			// TODO: handle slices?
			c <- datastore.Property{
				Name:  name,
				Value: value,
			}
		}
	}

	return nil
}

func (ctx *Context) PutAll(c *C, entities ...Entity) {
	// Get the keys for each Entity
	keys := make([]*datastore.Key, len(entities))
	for i, e := range entities {
		keys[i] = e.Key()
	}

	// Insert into the database
	_, err := datastore.PutMulti(ctx, keys, entities)
	c.Assert(err, IsNil)
}

func (ctx *Context) GetAll(c *C) map[string]Entity {
	// Get the keys for all currently stored Entities
	keys := ctx.allKeys(c)

	// Create a slice for the resulting values
	values := make([]Entity, len(keys))
	for i, key := range keys {
		values[i] = Entity{"__key__": key}
	}

	// Get the Entities
	err := datastore.GetMulti(ctx, keys, values)
	c.Assert(err, IsNil)

	// Convert the slices into a map to return
	all := make(map[string]Entity, len(keys))
	for i, k := range keys {
		all[k.Encode()] = values[i]
	}
	return all
}
