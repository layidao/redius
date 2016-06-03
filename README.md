# redius

Not Redis, is Redius 

对radix的封装

用例
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

	redius.SET("rediu", "this is rediu test")

	fmt.Println(redius.GET("rediu"))

}
```
