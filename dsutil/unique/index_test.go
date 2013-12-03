package unique

import (
	"appengine/datastore"
	"appengine/memcache"
	"testing"

	. "github.com/chippydip/gaege/testing"
	. "launchpad.net/gocheck"
)

// Hook up gocheck into the "go test" runner
func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&IndexSuite{})

type IndexSuite struct {
	Context
}

func (ctx *IndexSuite) SetUpSuite(c *C) {
	ctx.SetUp(c)
}

func (ctx *IndexSuite) TearDownSuite(c *C) {
	ctx.TearDown(c)
}

func (ctx *IndexSuite) TearDownTest(c *C) {
	ctx.Reset(c)
}

func (ctx *IndexSuite) TestIndex_GetValue_notFound(c *C) {
	idx := NewIndex("Test", 0)
	value, err := idx.GetValue(ctx, "not-found")
	c.Check(err, Equals, datastore.ErrNoSuchEntity)
	c.Check(value, Equals, "")
}

func (ctx *IndexSuite) TestIndex_GetValue_found(c *C) {
	ctx.PutAll(c, Entity{
		"__key__": ctx.Key("TestI", "id"),
		"$":       "value",
	})

	idx := NewIndex("Test", 0)
	value, err := idx.GetValue(ctx, "id")
	c.Check(err, IsNil)
	c.Check(value, Equals, "value")
}

func (ctx *IndexSuite) TestIndex_GetId_notFound(c *C) {
	idx := NewIndex("Test", 0)
	id, err := idx.GetId(ctx, "not-found")
	c.Check(err, Equals, datastore.ErrNoSuchEntity)
	c.Check(id, Equals, "")
}

func (ctx *IndexSuite) TestIndex_GetId_found(c *C) {
	ctx.PutAll(c, Entity{
		"__key__": ctx.Key("TestV", "value"),
		"$":       "id",
	})

	idx := NewIndex("Test", SaveOldValues) // don't check for failed deletes
	id, err := idx.GetId(ctx, "value")
	c.Check(err, IsNil)
	c.Check(id, Equals, "id")
}

func (ctx *IndexSuite) TestIndex_GetId_nonCanonical(c *C) {
	ctx.PutAll(c, Entity{
		"__key__": ctx.Key("TestV", "oldValue"),
		"$":       "id",
	}, Entity{
		"__key__": ctx.Key("TestV", "value"),
		"$":       "id",
	}, Entity{
		"__key__": ctx.Key("TestI", "id"),
		"$":       "value",
	})

	idx := NewIndex("Test", 0)
	id, err := idx.GetId(ctx, "oldValue")
	c.Check(err, Equals, datastore.ErrNoSuchEntity)
	c.Check(id, Equals, "")
	c.Check(ctx.GetAll(c), HasLen, 2) // oldValue should have been deleted
}

func (ctx *IndexSuite) TestIndex_Set_simple(c *C) {
	idx := NewIndex("Test", 0)
	err := idx.Set(ctx, "id", "value")
	c.Check(err, IsNil)

	id, err := idx.GetId(ctx, "value")
	c.Check(err, IsNil)
	c.Check(id, Equals, "id")

	value, err := idx.GetValue(ctx, "id")
	c.Check(err, IsNil)
	c.Check(value, Equals, "value")
}

func (ctx *IndexSuite) TestIndex_Set_inUse(c *C) {
	idx := NewIndex("Test", 0)
	err := idx.Set(ctx, "id1", "value")
	c.Check(err, IsNil)

	err = idx.Set(ctx, "id2", "value")
	c.Check(err, Equals, ErrDuplicateIndexValue)
}

func (ctx *IndexSuite) TestIndex_Set_reuseInUse(c *C) {
	idx := NewIndex("Test", SaveOldValues|PreventReuse)
	err := idx.Set(ctx, "id1", "oldValue")
	c.Check(err, IsNil)
	err = idx.Set(ctx, "id1", "value")
	c.Check(err, IsNil)
	c.Check(ctx.GetAll(c), HasLen, 3)

	err = idx.Set(ctx, "id2", "oldValue")
	c.Check(err, Equals, ErrDuplicateIndexValue)

	idx = NewIndex("Test", 0)
	err = idx.Set(ctx, "id2", "oldValue")
	c.Check(err, IsNil)
	c.Check(ctx.GetAll(c), HasLen, 4)

	id, err := idx.GetId(ctx, "oldValue")
	c.Check(err, IsNil)
	c.Check(id, Equals, "id2")
}

// Memcache tests

func (ctx *IndexSuite) TestIndex_GetValue_memcache(c *C) {
	err := memcache.Set(ctx, &memcache.Item{Key: "/TestI,id", Value: []byte("value")})
	c.Check(err, IsNil)

	idx := NewIndex("Test", 0)
	value, err := idx.GetValue(ctx, "id")
	c.Check(err, IsNil)
	c.Check(value, Equals, "value")
}

func (ctx *IndexSuite) TestIndex_GetId_memcache(c *C) {
	err := memcache.Set(ctx, &memcache.Item{Key: "/TestV,value", Value: []byte("id")})
	c.Check(err, IsNil)

	idx := NewIndex("Test", SaveOldValues) // don't check for failed deletes
	id, err := idx.GetId(ctx, "value")
	c.Check(err, IsNil)
	c.Check(id, Equals, "id")
}
