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

func (red *Redius) InitClient() error {
	//log.Printf("%d rediu initializing from %s:%s", red.Poolsize, red.Network, red.Addr)

	var err error
	if "" == red.Password {
		red._pool, err = pool.New(red.Network, red.Addr, red.Poolsize)
	} else {
		df := func(network, addr string) (*redis.Client, error) {
			client, err := redis.Dial(network, addr)
			if err != nil {
				return nil, err
			}
			if err = client.Cmd("AUTH", red.Password).Err; err != nil {
				client.Close()
				return nil, err
			}
			return client, nil
		}

		red._pool, err = pool.NewCustom(red.Network, red.Addr, red.Poolsize, df)
	}

	if err != nil {
		log.Println("Fatal:", err)
	}
	return err
}

func (p *Redius) GetClientFromPool() (*redis.Client, error) {
	return p._pool.Get()
}

func (p *Redius) PutClientToPool(c *redis.Client) {
	p._pool.Put(c)
}

// SET key
func (red *Redius) SET(key string, val string) error {
	c, err := red._pool.Get()
	if err != nil {
		return err
	}
	err = c.Cmd("SET", key, val).Err
	red._pool.Put(c)
	return err
}

// SETEX key seconds val
func (red *Redius) SETEX(key string, secs int, val string) error {
	c, err := red._pool.Get()
	if err != nil {
		return err
	}
	err = c.Cmd("SETEX", key, secs, val).Err
	red._pool.Put(c)
	return err
}

// GET key
func (red *Redius) GET(key string) (val string, err error) {
	c, err := red._pool.Get()
	if err != nil {
		return
	}
	val, err = c.Cmd("GET", key).Str()
	red._pool.Put(c)
	return
}

func (red *Redius) MGET(keys []string) (val []string, err error) {
	c, err := red._pool.Get()
	if err != nil {
		return
	}
	r := c.Cmd("MGET", keys)
	if r.Err != nil {
		return nil, r.Err
	}
	val, err = r.List()
	red._pool.Put(c)
	return
}

//
// hset key field val
func (red *Redius) HSET(key, field string, val string) error {
	c, err := red._pool.Get()
	if err != nil {
		return err
	}
	err = c.Cmd("HSET", key, field, val).Err
	red._pool.Put(c)
	return err
}

// HSETNX key field val timeout
func (red *Redius) HSETNX(key, field string, val string) (err error) {
	c, err := red._pool.Get()
	if err != nil {
		return
	}
	err = c.Cmd("HSETNX", key, field, val).Err
	red._pool.Put(c)
	return
}

// HMSET key field val [field val ...]
func (red *Redius) HMSET(key string, val map[string]interface{}) (err error) {
	c, err := red._pool.Get()
	if err != nil {
		return
	}
	err = c.Cmd("HMSET", key, val).Err
	red._pool.Put(c)
	return
}

// HGET key field
func (red *Redius) HGET(key, field string) (val string, err error) {
	c, err := red._pool.Get()
	if err != nil {
		return
	}
	val, err = c.Cmd("HGET", key, field).Str()
	red._pool.Put(c)
	return
}

// HMGET key field [field ...]
func (red *Redius) HMGET(key string, fields []string) (val []string, err error) {
	c, err := red._pool.Get()
	if err != nil {
		return
	}
	val, err = c.Cmd("HMGET", key, fields).List()
	red._pool.Put(c)
	return
}

func (red *Redius) HGETALL(key string) (val []string, err error) {
	c, err := red._pool.Get()
	if err != nil {
		return
	}
	val, err = c.Cmd("HGETALL", key).List()
	red._pool.Put(c)
	return
}

func (red *Redius) HKEYS(key string) (val []string, err error) {
	c, err := red._pool.Get()
	if err != nil {
		return
	}
	val, err = c.Cmd("HKEYS", key).List()
	red._pool.Put(c)
	return
}

// redis command:
// HDEL key field
func (red *Redius) HDEL(key, field string) error {
	c, err := red._pool.Get()
	if err != nil {
		return err
	}
	err = c.Cmd("HDEL", key, field).Err
	red._pool.Put(c)
	return err
}

// redis command:
// HDELALL key
func (red *Redius) HDELALL(key string) {
	c, err := red._pool.Get()
	if err != nil {
		return
	}
	_ = c.Cmd("DEL", key).Err
	red._pool.Put(c)
}

func (red *Redius) HINCRBY(key, field string, incr int) {
	c, err := red._pool.Get()
	if err != nil {
		return
	}
	_ = c.Cmd("HINCRBY", key, field, incr).Err
	red._pool.Put(c)
}

// redis list command:
// LRANGE
func (r *Redius) LRANGE(key string, start, stop int) (val []string, err error) {
	c, err := r._pool.Get()
	if err != nil {
		return nil, err
	}
	val, err = c.Cmd("LRANGE", key, start, stop).List()
	r._pool.Put(c)
	return val, err
}

func (r *Redius) LPUSH(key string, ids []int) error {
	c, err := r._pool.Get()
	if err != nil {
		return err
	}
	err = c.Cmd("LPUSH", key, ids).Err
	r._pool.Put(c)
	return err
}

func (r *Redius) RPOP(key string) (string, error) {
	c, err := r._pool.Get()
	if err != nil {
		return "0", err
	}
	val, err := c.Cmd("RPOP", key).Str()
	r._pool.Put(c)
	return val, err
}

// redis zset command:
// ZADD key score val
func (red *Redius) ZADD(key string, score int, val string) error {
	c, err := red._pool.Get()
	if err != nil {
		return err
	}
	err = c.Cmd("ZADD", key, score, val).Err
	red._pool.Put(c)
	return err
}

func (red *Redius) ZADDINT(key string, score int, val int) error {
	c, err := red._pool.Get()
	if err != nil {
		return err
	}
	err = c.Cmd("ZADD", key, score, val).Err
	red._pool.Put(c)
	return err
}

// ZINCRBY key increment member
func (red *Redius) ZINCRBY(key string, increment int, val string) error {
	c, err := red._pool.Get()
	if err != nil {
		return err
	}
	err = c.Cmd("ZINCRBY", key, increment, val).Err
	red._pool.Put(c)
	return err
}

// ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
func (red *Redius) ZREVRANGEBYSCORE(key string, start, step int, withsocres bool) (val []string, err error) {
	c, err := red._pool.Get()
	if err != nil {
		return nil, err
	}

	if withsocres {
		val, err = c.Cmd("ZREVRANGEBYSCORE", key, "+inf", "-inf", "WITHSCORES", "LIMIT", start, step).List()
	} else {
		val, err = c.Cmd("ZREVRANGEBYSCORE", key, "+inf", "-inf", "LIMIT", start, step).List()
	}

	red._pool.Put(c)
	return
}

// 返回有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员。按 score 值递增(从小到大)次序排列。
// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
func (red *Redius) ZRANGEBYSCORE(key string, min, max, start, step int, withsocres bool) (val []string, err error) {
	c, err := red._pool.Get()
	if err != nil {
		return nil, err
	}
	if withsocres {
		val, err = c.Cmd("ZRANGEBYSCORE", key, min, max, "WITHSCORES", "LIMIT", start, step).List()
	} else {
		val, err = c.Cmd("ZRANGEBYSCORE", key, min, max, "LIMIT", start, step).List()
	}
	red._pool.Put(c)
	return
}

// 删除
// ZREMRANGEBYSCORE key min max
func (red *Redius) ZREMRANGEBYSCORE(key string, min, max int) (err error) {
	c, err := red._pool.Get()
	if err != nil {
		return err
	}

	err = c.Cmd("ZREMRANGEBYSCORE", key, min, max).Err

	red._pool.Put(c)
	return err
}

// KEY 操作
func (red *Redius) DEL(key string) (err error) {
	c, err := red._pool.Get()
	if err != nil {
		return err
	}
	err = c.Cmd("DEL", key).Err
	red._pool.Put(c)
	return err
}

func (red *Redius) DELALL(keys []string) (err error) {
	c, err := red._pool.Get()
	if err != nil {
		return err
	}
	err = c.Cmd("DEL", keys).Err
	red._pool.Put(c)
	return err
}

// EXPIREAT key timestamp
func (red *Redius) EXPIREAT(key string, timestamp int) {
	c, err := red._pool.Get()
	if err != nil {
		return
	}
	_ = c.Cmd("EXPIREAT", key, timestamp).Err
	red._pool.Put(c)
}

func (red *Redius) KEYS(key string) (val []string, err error) {
	c, err := red._pool.Get()
	if err != nil {
		return nil, err
	}
	val, err = c.Cmd("KEYS", key).List()
	red._pool.Put(c)
	return
}

func (red *Redius) SCAN(cursor, pattern, count string) (val []string, err error) {
	c, err := red._pool.Get()
	if err != nil {
		return nil, err
	}
	val, err = c.Cmd("SCAN", cursor, "MATCH", pattern, "COUNT", count).List()
	red._pool.Put(c)
	return
}

func (red *Redius) HSCAN(cursor, pattern, count string) (val []string, err error) {
	c, err := red._pool.Get()
	if err != nil {
		return nil, err
	}
	val, err = c.Cmd("HSCAN", cursor, "MATCH", pattern, "COUNT", count).List()
	red._pool.Put(c)
	return
}
