package sync

import (
	"fmt"
	"strings"

	"github.com/garyburd/redigo/redis"
)

func Sync(src, dst redis.Conn) (err error) {
	keys, _ := redis.Strings(src.Do("KEYS", "*"))

	err = dst.Send("MULTI")
	if err != nil {
		return err
	}

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
			return fmt.Errorf("Unknown TYPE: '%s' for KEY '%s'\n", t, key)
		}
	}

	_, err = dst.Do("EXEC")
	if err != nil {
		return err
	}

	return nil
}

func prepend(key string, args []interface{}) []interface{} {
	keyAndArgs := make([]interface{}, len(args)+1)

	keyAndArgs[0] = key
	for i := range args {
		keyAndArgs[i+1] = args[i]
	}

	return keyAndArgs
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

	err = dst.Send("SET", key, value)
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

	args := prepend(key, list)
	err = dst.Send("RPUSH", args...)
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

	args := prepend(key, set)
	err = dst.Send("SADD", args...)
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

	args := prepend(key, reverse(sortedSet))
	err = dst.Send("ZADD", args...)
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

	args := prepend(key, hash)
	err = dst.Send("HMSET", args...)
	if err != nil {
		return err
	}

	return nil
}
