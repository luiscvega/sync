package sync

import (
	"fmt"
	"strings"

	"github.com/garyburd/redigo/redis"
)

func Sync(src, dst redis.Conn) (err error) {
	keys, _ := redis.Strings(src.Do("KEYS", "*"))

	for _, key := range keys {
		fmt.Printf("-----> %s\n", key)

		t, err := redis.String(src.Do("TYPE", key))
		if err != nil {
			return err
		}

		switch strings.ToUpper(t) {
		case "STRING":
			err = copyString(key, src, dst)
			if err != nil {
				return err
			}
		case "LIST":
			err = copyList(key, src, dst)
			if err != nil {
				return err
			}
		case "SET":
			err = copySet(key, src, dst)
			if err != nil {
				return err
			}
		case "ZSET":
			err = copySortedSet(key, src, dst)
			if err != nil {
				return err
			}
		case "HASH":
			err = copyHash(key, src, dst)
			if err != nil {
				return err
			}
		default:
			fmt.Printf("Unknown TYPE: '%s' for KEY '%s'\n", t, key)
		}
	}

	return nil
}

func prepend(key string, args []interface{}) []interface{} {
	keysAndArgs := make([]interface{}, len(args)+1)

	keysAndArgs[0] = key
	for i := range args {
		keysAndArgs[i+1] = args[i]
	}

	return keysAndArgs
}

// Only for Sorted Sets due to sequence of arguments for scores
func reverse(args []interface{}) []interface{} {
	reversed := make([]interface{}, len(args))

	j := 0
	for i := len(args) - 1; i >= 0; i-- {
		reversed[j] = args[i]
		j++
	}

	return reversed
}

func copyString(key string, src redis.Conn, dst redis.Conn) (err error) {
	value, err := src.Do("GET", key)
	if err != nil {
		return err
	}

	_, err = dst.Do("SET", key, value)
	if err != nil {
		return err
	}

	return nil
}

func copyList(key string, src redis.Conn, dst redis.Conn) (err error) {
	list, err := redis.Values(src.Do("LRANGE", key, 0, -1))
	if err != nil {
		return err
	}

	dst.Send("MULTI")
	args := prepend(key, list)
	dst.Send("RPUSH", args...)
	_, err = dst.Do("EXEC")
	if err != nil {
		return err
	}

	return nil
}

func copySet(key string, src redis.Conn, dst redis.Conn) (err error) {
	set, _ := redis.Values(src.Do("SMEMBERS", key))
	if err != nil {
		return err
	}

	dst.Send("MULTI")
	args := prepend(key, set)
	dst.Send("SADD", args...)
	_, err = dst.Do("EXEC")
	if err != nil {
		return err
	}

	return nil
}

func copySortedSet(key string, src redis.Conn, dst redis.Conn) (err error) {
	sortedSet, err := redis.Values(src.Do("ZRANGE", key, 0, -1, "WITHSCORES"))
	if err != nil {
		return err
	}

	dst.Send("MULTI")
	args := prepend(key, reverse(sortedSet))
	dst.Send("ZADD", args...)
	_, err = dst.Do("EXEC")
	if err != nil {
		return err
	}

	return nil
}

func copyHash(key string, src redis.Conn, dst redis.Conn) (err error) {
	hash, err := redis.Values(src.Do("HGETALL", key))
	if err != nil {
		return err
	}

	dst.Send("MULTI")
	args := prepend(key, hash)
	dst.Send("HMSET", args...)
	_, err = dst.Do("EXEC")
	if err != nil {
		return err
	}

	return nil
}
