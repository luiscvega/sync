package sync

import (
	"fmt"

	"github.com/garyburd/redigo/redis"
)

func Sync(src, dst redis.Conn) (err error) {
	keys, _ := redis.Strings(src.Do("KEYS", "*"))

	for _, key := range keys {
		fmt.Printf("-----> %s\n", key)

		t, err := src.Do("TYPE", key)
		if err != nil {
			return err
		}

		switch t {
		case "string":
			err = copyString(key, src, dst)
			if err != nil {
				return err
			}
		case "list":
			err = copyList(key, src, dst)
			if err != nil {
				return err
			}
		case "set":
			err = copySet(key, src, dst)
			if err != nil {
				return err
			}
		case "zset":
			err = copySortedSet(key, src, dst)
			if err != nil {
				return err
			}
		case "hash":
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
	for _, elem := range list {
		err = dst.Send("RPUSH", key, elem)
		if err != nil {
			return err
		}
	}

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
	for _, member := range set {
		err = dst.Send("SADD", key, member)
		if err != nil {
			return err
		}
	}

	_, err = dst.Do("EXEC")
	if err != nil {
		return err
	}

	return nil
}

func copySortedSet(key string, src redis.Conn, dst redis.Conn) (err error) {
	sortedSet, _ := redis.Values(src.Do("ZRANGE", key, 0, -1, "WITHSCORES"))
	if err != nil {
		return err
	}

	dst.Send("MULTI")
	for i, member := range sortedSet {
		if i%2 == 0 {
			err = dst.Send("ZADD", key, sortedSet[i+1], member)
			if err != nil {
				return err
			}
		}
	}

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
	for i, hashKey := range hash {
		if i%2 == 0 {
			err = dst.Send("HSET", key, hashKey, hash[i+1])
			if err != nil {
				return err
			}
		}
	}

	_, err = dst.Do("EXEC")
	if err != nil {
		return err
	}

	return nil
}
