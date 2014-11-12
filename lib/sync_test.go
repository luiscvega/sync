package sync

import (
	"reflect"
	"sort"
	"testing"

	"github.com/garyburd/redigo/redis"
)

func flushThenClose(conn redis.Conn) {
	conn.Do("FLUSHDB")
	conn.Close()
}

func TestSync(t *testing.T) {
	src, _ := redis.Dial("tcp", "localhost:6379")
	defer flushThenClose(src)

	dst, _ := redis.Dial("tcp", "localhost:6380")
	defer flushThenClose(dst)

	src.Do("SET", "string", "one")                                           // String
	src.Do("LPUSH", "list", "one", "two", "three")                           // List
	src.Do("SADD", "set", "one", "two", "three")                             // Set
	src.Do("ZADD", "sorted_set", 1, "one", 2, "two", 3, "three")             // Sorted Set
	src.Do("HMSET", "hash", "one", "isa", "two", "dalawa", "three", "tatlo") // Sorted Set

	err := Sync(src, dst)
	if err != nil {
		t.Error(err)
		return
	}

	// Check String
	expectedString, _ := redis.String(src.Do("GET", "string"))
	actualString, _ := redis.String(dst.Do("GET", "string"))
	if actualString != expectedString {
		t.Errorf("%s != %s", actualString, expectedString)
	}

	// Check List
	expectedList, _ := redis.Values(src.Do("LRANGE", "list", 0, -1))
	actualList, _ := redis.Values(dst.Do("LRANGE", "list", 0, -1))
	if !reflect.DeepEqual(expectedList, actualList) {
		t.Error("Failed sync for TYPE 'LIST'")
	}

	// Check Set
	expectedSet, _ := convertToSlice(src.Do("SMEMBERS", "set"))
	sort.Strings(expectedSet)
	actualSet, _ := convertToSlice(dst.Do("SMEMBERS", "set"))
	sort.Strings(actualSet)
	if !reflect.DeepEqual(expectedSet, actualSet) {
		t.Error("Failed sync for TYPE 'SET'")
	}

	// Check Sorted Set
	expectedSortedSet, _ := redis.Values(src.Do("ZRANGE", "sorted_set", 0, -1, "WITHSCORES"))
	actualSortedSet, _ := redis.Values(dst.Do("ZRANGE", "sorted_set", 0, -1, "WITHSCORES"))
	if !reflect.DeepEqual(expectedSortedSet, actualSortedSet) {
		t.Error("Failed sync for TYPE 'ZSET'")
	}

	// Check Hash
	expectedHash, _ := convertToMap(src.Do("HGETALL", "hash"))
	actualHash, _ := convertToMap(dst.Do("HGETALL", "hash"))
	if !reflect.DeepEqual(expectedHash, actualHash) {
		t.Error("Failed sync for TYPE 'HASH'")
	}
}

func convertToSlice(reply interface{}, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}

	slice := make([]string, 0)

	values, _ := redis.Values(reply, err)
	for _, value := range values {
		slice = append(slice, toString(value))
	}

	return slice, nil
}

func convertToMap(reply interface{}, err error) (map[string]string, error) {
	if err != nil {
		return nil, err
	}

	hash := make(map[string]string, 0)

	values, _ := redis.Values(reply, err)
	for i, value := range values {
		if i%2 == 0 {
			hash[toString(value)] = toString(values[i+1])
		}
	}

	return hash, nil
}

func toString(value interface{}) string {
	return string(value.([]byte))
}
