package redius

import (
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mediocregopher/radix.v2/redis"
	"log"
)

type Redius struct {
	Addr     string
	Network  string
	Poolsize int
	Password string
	_pool    *pool.Pool
}

func (this *Redius) InitClient() {
	//log.Printf("%d rediu initializing from %s:%s", this.Poolsize, this.Network, this.Addr)

	var err error
	if "" == this.Password {
		this._pool, err = pool.New(this.Network, this.Addr, this.Poolsize)
	} else {
		df := func(network, addr string) (*redis.Client, error) {
			client, err := redis.Dial(network, addr)
			if err != nil {
				return nil, err
			}
			if err = client.Cmd("AUTH", this.Password).Err; err != nil {
				client.Close()
				return nil, err
			}
			return client, nil
		}

		this._pool, err = pool.NewCustom(this.Network, this.Addr, this.Poolsize, df)
	}

	if err != nil {
		log.Println(err)
	}
}

// SET key
func (this *Redius) SET(key string, val string) error {
	c, err := this._pool.Get()
	if err != nil {
		return err
	}
	err = c.Cmd("SET", key, val).Err
	this._pool.Put(c)
	return err
}

// SETEX key seconds val
func (this *Redius) SETEX(key string, secs int, val string) error {
	c, err := this._pool.Get()
	if err != nil {
		return err
	}
	err = c.Cmd("SETEX", key, secs, val).Err
	this._pool.Put(c)
	return err
}

// GET key
func (this *Redius) GET(key string) (val string, err error) {
	c, err := this._pool.Get()
	if err != nil {
		return
	}
	val, err = c.Cmd("GET", key).Str()
	this._pool.Put(c)
	return
}

func (this *Redius) MGET(keys []string) (val []string, err error) {
	c, err := this._pool.Get()
	if err != nil {
		return
	}
	r := c.Cmd("MGET", keys)
	if r.Err != nil {
		return nil, r.Err
	}
	val, err = r.List()
	this._pool.Put(c)
	return
}

//
// hset key field val
func (this *Redius) HSET(key, field string, val string) error {
	c, err := this._pool.Get()
	if err != nil {
		return err
	}
	err = c.Cmd("HSET", key, field, val).Err
	this._pool.Put(c)
	return err
}

// HSETNX key field val timeout
func (this *Redius) HSETNX(key, field string, val string) (err error) {
	c, err := this._pool.Get()
	if err != nil {
		return
	}
	err = c.Cmd("HSETNX", key, field, val).Err
	this._pool.Put(c)
	return
}

// HMSET key field val [field val ...]
func (this *Redius) HMSET(key string, val map[string]interface{}) (err error) {
	c, err := this._pool.Get()
	if err != nil {
		return
	}
	err = c.Cmd("HMSET", key, val).Err
	this._pool.Put(c)
	return
}

// HGET key field
func (this *Redius) HGET(key, field string) (val string, err error) {
	c, err := this._pool.Get()
	if err != nil {
		return
	}
	val, err = c.Cmd("HGET", key, field).Str()
	this._pool.Put(c)
	return
}

// HMGET key field [field ...]
func (this *Redius) HMGET(key string, fields []string) (val []string, err error) {
	c, err := this._pool.Get()
	if err != nil {
		return
	}
	val, err = c.Cmd("HMGET", key, fields).List()
	this._pool.Put(c)
	return
}

func (this *Redius) HGETALL(key string) (val []string, err error) {
	c, err := this._pool.Get()
	if err != nil {
		return
	}
	val, err = c.Cmd("HGETALL", key).List()
	this._pool.Put(c)
	return
}

// redis command:
// HDEL key field
func (this *Redius) HDEL(key, field string) error {
	c, err := this._pool.Get()
	if err != nil {
		return err
	}
	err = c.Cmd("HDEL", key, field).Err
	this._pool.Put(c)
	return err
}

// redis command:
// HDELALL key
func (this *Redius) HDELALL(key string) {
	c, err := this._pool.Get()
	if err != nil {
		return
	}
	_ = c.Cmd("DEL", key).Err
	this._pool.Put(c)
}

// redis zset command:
// ZADD key score val
func (this *Redius) ZADD(key string, score int, val string) error {
	c, err := this._pool.Get()
	if err != nil {
		return err
	}
	err = c.Cmd("ZADD", key, score, val).Err
	this._pool.Put(c)
	return err
}

func (this *Redius) ZADDINT(key string, score int, val int) error {
	c, err := this._pool.Get()
	if err != nil {
		return err
	}
	err = c.Cmd("ZADD", key, score, val).Err
	this._pool.Put(c)
	return err
}

// ZINCRBY key increment member
func (this *Redius) ZINCRBY(key string, increment int, val string) error {
	c, err := this._pool.Get()
	if err != nil {
		return err
	}
	err = c.Cmd("ZINCRBY", key, increment, val).Err
	this._pool.Put(c)
	return err
}

// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
func (this *Redius) ZREVRANGEBYSCORE(key string, start, step int, withsocres bool) (val []string, err error) {
	c, err := this._pool.Get()
	if err != nil {
		return nil, err
	}

	if withsocres {
		val, err = c.Cmd("ZREVRANGEBYSCORE", key, "+inf", "-inf", "WITHSCORES", "LIMIT", start, step).List()
	} else {
		val, err = c.Cmd("ZREVRANGEBYSCORE", key, "+inf", "-inf", "LIMIT", start, step).List()
	}

	this._pool.Put(c)
	return
}

// 返回有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员。按 score 值递增(从小到大)次序排列。
// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
func (this *Redius) ZRANGEBYSCORE(key string, min, max, start, step int, withsocres bool) (val []string, err error) {
	c, err := this._pool.Get()
	if err != nil {
		return nil, err
	}
	if withsocres {
		val, err = c.Cmd("ZRANGEBYSCORE", key, min, max, "WITHSCORES", "LIMIT", start, step).List()
	} else {
		val, err = c.Cmd("ZRANGEBYSCORE", key, min, max, "LIMIT", start, step).List()
	}
	this._pool.Put(c)
	return
}

// 删除
// ZREMRANGEBYSCORE key min max
func (this *Redius) ZREMRANGEBYSCORE(key string, min, max int) (err error) {
	c, err := this._pool.Get()
	if err != nil {
		return err
	}

	err = c.Cmd("ZREMRANGEBYSCORE", key, min, max).Err

	this._pool.Put(c)
	return err
}

// KEY 操作
func (this *Redius) DEL(key string) (err error) {
	c, err := this._pool.Get()
	if err != nil {
		return err
	}
	err = c.Cmd("DEL", key).Err
	this._pool.Put(c)
	return err
}

func (this *Redius) DELALL(keys []string) (err error) {
	c, err := this._pool.Get()
	if err != nil {
		return err
	}
	err = c.Cmd("DEL", keys).Err
	this._pool.Put(c)
	return err
}

// EXPIREAT key timestamp
func (this *Redius) EXPIREAT(key string, timestamp int) {
	c, err := this._pool.Get()
	if err != nil {
		return
	}
	_ = c.Cmd("EXPIREAT", key, timestamp).Err
	this._pool.Put(c)
}

func (this *Redius) KEYS(key string) (val []string, err error) {
	c, err := this._pool.Get()
	if err != nil {
		return nil, err
	}
	val, err = c.Cmd("KEYS", key).List()
	this._pool.Put(c)
	return
}
