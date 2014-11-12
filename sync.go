package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/garyburd/redigo/redis"

	sync "./lib"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("usage: %s <src> <dst>\n", filepath.Base(os.Args[0]))
		os.Exit(1)
	}

	src, err := redis.Dial("tcp", os.Args[1])
	if err != nil {
		fmt.Println(err)
		return
	}

	dst, err := redis.Dial("tcp", os.Args[2])
	if err != nil {
		fmt.Println(err)
		return
	}

	err = sync.Sync(src, dst)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("SYNC COMPLETE")
}
