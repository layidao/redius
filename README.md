# redius

Not Redis, is Redius 

pacakge radix, more convenient to use redis

eg: main.go
```
package main

import (
	"fmt"
	"github.com/layidao/redius"
)

func main() {

	redius := &redius.Redius{
		Addr:     "127.0.0.1:6379",
		Network:  "tcp",
		Poolsize: 10,
		Password: "",
	}

	redius.InitClient()

	redius.SET("redis", "this is redius,that is redis")

	fmt.Println(redius.GET("redis"))

}
```
