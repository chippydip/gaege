package testing

import (
	"appengine/datastore"

	. "launchpad.net/gocheck"
)

var _ = Suite(&DsSuite{})

type DsSuite struct {
	Context
}

func (ctx *DsSuite) SetUpSuite(c *C) {
	ctx.SetUp(c)
}

func (ctx *DsSuite) TearDownSuite(c *C) {
	ctx.TearDown(c)
}

func (ctx *DsSuite) TearDownTest(c *C) {
	ctx.Reset(c)
}

func (ctx *DsSuite) TestContext_Key_valid(c *C) {
	tests := []struct {
		key      *datastore.Key
		kind     string
		stringID string
		intID    int64
	}{
		{ctx.Key("kind", 10), "kind", "", 10},
		{ctx.Key("kind", byte(10)), "kind", "", 10},
		{ctx.Key("kind", uint64(10)), "kind", "", 10},
		{ctx.Key("kind", "id"), "kind", "id", 0},
		{ctx.Key("kind", "10"), "kind", "10", 0},
		{ctx.Key("kind", uint64(1<<63)), "kind", "", -9223372036854775808},
	}

	for i, t := range tests {
		iter := Commentf("Test case #%d failed: %+v", i, t)
		c.Check(t.key, KeyEquals, t.kind, t.stringID, t.intID, false, iter)
	}
}

func (ctx *DsSuite) TestContext_Key_parent(c *C) {
	key := ctx.Key("parent", 1, "child", 2)

	c.Check(key, KeyEquals, "child", "", 2, true)
	c.Check(key.Parent(), KeyEquals, "parent", "", 1, false)
}

func (ctx *DsSuite) TestContext_Key_invalid(c *C) {
	tests := [][]interface{}{
		{},
		{"kind"},
		{10, 10},
		{"parent", 10, "child"},
	}

	for i, t := range tests {
		iter := Commentf("Test case #%d failed: %+v", i, t)
		c.Check(ctx.Key(t...), IsNil, iter)
	}
}

func (ctx *DsSuite) TestContext_PutAll_simple(c *C) {
	e := Entity{
		"__key__": ctx.Key("Test", 10),
		"Name":    "Value",
	}

	ctx.PutAll(c, e)

	var props datastore.PropertyList
	err := datastore.Get(ctx, e.Key(), &props)
	c.Check(err, IsNil)
	c.Check(props, HasLen, 1)
	c.Check(props[0], Equals, datastore.Property{Name: "Name", Value: "Value"})
}

func (ctx *DsSuite) TestContext_GetAll_simple(c *C) {
	e := Entity{
		"__key__": ctx.Key("Test", 10),
		"Name":    "Value",
	}

	props := datastore.PropertyList{datastore.Property{Name: "Name", Value: "Value"}}
	_, err := datastore.Put(ctx, e.Key(), &props)
	c.Assert(err, IsNil)

	all := ctx.GetAll(c)
	c.Check(all, HasLen, 1)
	c.Check(all[e.Key().Encode()], DeepEquals, e)
}

// func (ctx *DsSuite) TestContext_GetAll_many(c *C) {
// 	n := 1001
// 	for i := 0; i < n; i++ {
// 		_, err := datastore.Put(ctx, ctx.Key("kind", i), Entity{})
// 		c.Assert(err, IsNil)
// 	}

// 	all := ctx.GetAll(c)
// 	c.Check(all, HasLen, n)
// }
