package sharded

import (
	"appengine"
	"appengine/datastore"
	"math/rand"

	"github.com/chippydip/gaege/dsutil"
)

type Sharded struct {
	// Name of the sharded entity group (must not be empty).
	name string

	// Desired number of shards to spread writes over.
	count int
}

// Create a new Shards object with the given name and at least `count` shards.
func New(name string, count int) Sharded {
	if name == "" {
		panic("name is required")
	}

	// Make sure at least one shard is used
	if count < 1 {
		count = 1
	}

	return Sharded{
		name:  name,
		count: count,
	}
}

// Name returns the name that was passed to New.
func (s Sharded) Name() string {
	return s.name
}

// Count returns the count that was passed to New.
func (s Sharded) Count() int {
	return s.count
}

// UpdateRand is a convenience method that starts a transaction, selects a
// random shard, and then calls the given function with the randomly selected
// shard's key.
func (s Sharded) UpdateRand(ctx appengine.Context, update func(appengine.Context, *datastore.Key) error) error {
	return dsutil.RunInTransaction(ctx, func(ctx appengine.Context) error {
		// Get a random shard to update
		key, err := s.Rand(ctx)
		if err != nil {
			return err
		}

		// Call the update function
		return update(ctx, key)
	})
}

// Rand selects a random shard and return its key. It must be called from
// within a cross-group transaction.
func (s Sharded) Rand(ctx appengine.Context) (*datastore.Key, error) {
	// Get the shard config
	cfg, err := s.config(ctx, true)
	if err != nil {
		return nil, err
	}

	// Pick a shard [1, count] and generate a parent key for that shard
	return s.key(ctx, rand.Intn(cfg.Count)+1), nil
}

// All returns a slice of all sharded keys that should be read from.
func (s Sharded) All(ctx appengine.Context) ([]*datastore.Key, error) {
	// Get the shard config
	cfg, err := s.config(ctx, false)
	if err != nil {
		return nil, err
	}

	// Create keys for all shards
	keys := make([]*datastore.Key, cfg.Max)
	for i := 0; i < cfg.Max; i++ {
		keys[i] = s.key(ctx, i+1)
	}
	return keys, nil
}

// Vacuum allows the number of shards to be decreased. By default, the config
// keeps track of the largest number of shards that have ever been used and
// performs reads on all of these shards while writes only use the currently
// configured number of shards.
// The given merge function should read all data from `src` and add it to `dst`
// and then delete all data from `src`. If the count is ever increased again
// it is important that all data is actually deleted so that it doesn't
// accidentally get picked up again. It is called repeatedly to merge as single
// pair of shards until Max == Count.
// TODO: Limited batching could be supported, but the transaction must already
// use 3 different entity groups, so the max batch size would be 3 (for a
// total of 5 entity groups in the transaction).
func (s Sharded) Vacuum(ctx appengine.Context, merge func(c appengine.Context, src, dst *datastore.Key) error) error {
	done := false
	for {
		// Use a separate transaction for each merge
		err := dsutil.RunInTransaction(ctx, func(ctx appengine.Context) error {
			// Get the shard config
			cfg, err := s.config(ctx, true)
			if err != nil {
				return err
			}

			// Stop if there is nothing that needs to be merged
			if cfg.Max <= cfg.Count {
				done = true
				return nil
			}

			// Merge the last shard into a randomly selected one
			src := s.key(ctx, cfg.Max)
			dst := s.key(ctx, rand.Intn(cfg.Count)+1)
			if err := merge(ctx, src, dst); err != nil {
				return err
			}

			// Decrement the max value and save
			cfg.Max--
			cfgKey := s.cfgKey(ctx)
			if _, err := datastore.Put(ctx, cfgKey, cfg); err != nil {
				return err
			}

			// Check the stopping condition again to save a transaction
			// and initial config read if we are done at this point.
			if cfg.Max <= cfg.Count {
				done = true
			}
			return nil
		})
		if done || err != nil {
			return err
		}
	}
}

// Create a key for the i-th shard of the data.
func (s Sharded) key(ctx appengine.Context, i int) *datastore.Key {
	return datastore.NewKey(ctx, s.name, "", int64(i), nil)
}

//////////////////////////////////////////////////////////////////////////////

// Entity name for shard configuration.
const kShardConfigKind = "ShardConfig"

// Shard configuration data.
type shardConfig struct {
	// Current number of shards that are being written to.
	Count int `datastore:",noindex"`

	// Total number of shards that have ever been written to.
	// This should be the largest value Count has ever had.
	Max int `datastore:",noindex"`
}

// Create a key for this shards config entity.
func (s Sharded) cfgKey(ctx appengine.Context) *datastore.Key {
	return datastore.NewKey(ctx, kShardConfigKind, s.name, 0, nil)
}

// Get the current config, optionally updating the stored values. In an update is
// requested, then the method should be called from within a transaction context.
func (s Sharded) config(ctx appengine.Context, update bool) (cfg shardConfig, err error) {
	// Get the configuration key
	cfgKey := s.cfgKey(ctx)

	// Read the current config
	if err = datastore.Get(ctx, cfgKey, &cfg); err != nil && err != datastore.ErrNoSuchEntity {
		return cfg, err
	}
	if cfg.Count > cfg.Max {
		panic("sharded: invalid configuration found in datastore (Count > Max)")
	}

	// Get the max value
	max := 0
	for _, v := range [...]int{s.count, cfg.Count, cfg.Max} {
		if v > max {
			max = v
		}
	}

	// Update the config if required. This is calculation is repeatable for reads
	// and writes should be using update == true to store any changes made here.
	modified := false
	if cfg.Count != s.count {
		cfg.Count = s.count
		if cfg.Count > cfg.Max {
			cfg.Max = cfg.Count
		}
		modified = true
	}

	// Update if requested
	if update {
		if !dsutil.IsInTransaction(ctx) {
			panic("sharded: must be called from a transaction if update is true")
		}

		// But only if the configuration needs to be changed
		if modified {
			if _, err := datastore.Put(ctx, cfgKey, &cfg); err != nil {
				return cfg, err
			}
		}
	}

	return cfg, nil
}
