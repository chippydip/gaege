package testing

import (
	"appengine"
	"appengine/datastore"
	"appengine/memcache"
	"testing"

	. "launchpad.net/gocheck"
)

// Hook up gocheck into the "go test" runner
func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&MyContextSuite{})

type MyContextSuite struct {
	Context
}

func (ctx *MyContextSuite) SetUpSuite(c *C) {
	ctx.SetUp(c)
	c.Check(appengine.AppID(ctx), Equals, "testapp")
}

func (ctx *MyContextSuite) TearDownSuite(c *C) {
	ctx.TearDown(c)
	c.Check(ctx.Context.Context, IsNil)
}

func (ctx *MyContextSuite) TearDownTest(c *C) {
	ctx.Reset(c)
}

func (ctx *MyContextSuite) TestContext_Reset_datastore(c *C) {
	key := datastore.NewKey(ctx, "Entity", "", 0, nil) // NB: test incomplete key
	key, err := datastore.Put(ctx, key, Entity{})
	c.Check(err, IsNil)
	c.Check(ctx.DsCount(), Equals, 1)

	ctx.Reset(c)
	c.Check(ctx.DsCount(), Equals, 0)

	var result Entity
	err = datastore.Get(ctx, key, &result)
	c.Check(err, Equals, datastore.ErrNoSuchEntity)
}

func (ctx *MyContextSuite) TestContext_Reset_memcache(c *C) {
	err := memcache.Set(ctx, &memcache.Item{Key: "key", Value: []byte("value")})
	c.Check(err, IsNil)
	c.Check(ctx.McCount(), Equals, 1)

	ctx.Reset(c)
	c.Check(ctx.McCount(), Equals, 0)

	item, err := memcache.Get(ctx, "key")
	c.Check(err, Equals, memcache.ErrCacheMiss)
	c.Check(item, IsNil)
}

// TODO: test KeyEquals?
