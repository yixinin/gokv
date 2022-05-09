package gokv

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/yixinin/gokv/codec"
	"github.com/yixinin/gokv/kvstore"
)

type HashIface interface {
	HDel(key string, fields ...string) error
	HExists(key, field string) (bool, error)
	HGet(key, field string) (string, error)
	HGetAll(key string) (map[string]string, error)
	HIncrBy(key, field string, incr int64) (int64, error)
	HIncrByFloat(key, field string, incr float64) (float64, error)
	HKeys(key string) ([]string, error)
	HLen(key string) (int, error)
	HMGet(key string, fields ...string) ([]string, error)
	HMSet(key string, fields map[string]interface{}) (string, error)
	HSet(key, field string, value interface{}) (bool, error)
	HSetNX(key, field string, value interface{}) (bool, error)
	HVals(key string) ([]string, error)
}

const SplitC = ':'
const HashKeySplitC = ','

func sliceEq(s1, s2 []byte) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i := range s1 {
		if s1[i] != s2[i] {
			return false
		}
	}
	return true
}
func readFields(hkval []byte) []string {
	var fields = make([]string, 0, 2)
	var i, prev int
	for i < len(hkval) {
		if hkval[i] == HashKeySplitC {
			fields = append(fields, string(hkval[prev:i]))
			i++
			prev = i
		}
		i++
	}
	if prev < i {
		fields = append(fields, string(hkval[prev:i]))
	}
	return fields
}
func genFields(key string, fields []string) []byte {
	s := []byte(strings.Join(fields, ","))
	val := fmt.Sprintf("h:%s:%s", key, s)
	return []byte(val)
}

type _hashImpl struct {
	_db kvstore.Kvstore
}

func (h *_hashImpl) hCheckKey(ctx context.Context, key []byte) error {
	hkval, err := h._db.Get(ctx, key)
	switch err {
	case nil:
		if len(hkval) <= len(key)+2 || hkval[len(key)+2] != SplitC {
			return ErrKeyOPType
		}
		if hkval[0] != 'h' || hkval[1] != ':' {
			return ErrKeyOPType
		}
		if !sliceEq(hkval[2:2+len(key)], key) {
			return ErrKeyOPType
		}
		return nil
	case ErrNotfound:
		return nil
	default:
		return err
	}
}

func (h *_hashImpl) hCheckKeyForUpdate(ctx context.Context, key, field string) error {
	var bKey = []byte(key)
	hkval, err := h._db.Get(ctx, bKey)
	switch err {
	case nil:
		if len(hkval) <= len(key)+2 || hkval[len(key)+2] != SplitC {
			return ErrKeyOPType
		}
		if hkval[0] != 'h' || hkval[1] != ':' {
			return ErrKeyOPType
		}
		if !sliceEq(hkval[2:2+len(key)], bKey) {
			return ErrKeyOPType
		}
		fields := readFields(hkval[2+len(key)+1:])
		for _, f := range fields {
			if f == field {
				return nil
			}
		}
		fields = append(fields, field)
		return h._db.Set(ctx, bKey, genFields(key, fields))
	case ErrNotfound:
		val := genFields(key, []string{field})
		return h._db.Set(ctx, bKey, val)
	default:
		return err
	}
}

func (h *_hashImpl) hGetAllKeys(ctx context.Context, key string) ([][]byte, error) {
	var bKey = []byte(key)
	hkval, err := h._db.Get(ctx, bKey)
	if err != nil {
		return nil, err
	}

	if len(hkval) <= len(key)+2 || hkval[len(key)+2] != SplitC {
		return nil, ErrKeyOPType
	}
	if hkval[0] != 'h' || hkval[1] != ':' {
		return nil, ErrKeyOPType
	}
	if !sliceEq(hkval[2:2+len(key)], bKey) {
		return nil, ErrKeyOPType
	}
	fields := readFields(hkval[2+len(key)+1:])
	var keys = make([][]byte, 0, len(fields))
	for _, f := range fields {
		keys = append(keys, genHashFieldKey(key, f))
	}
	return keys, nil
}

func genHashFieldKey(key, field string) []byte {
	fkey := fmt.Sprintf("h:%s:%s", key, field)
	return []byte(fkey)
}
func (h *_hashImpl) HSet(ctx context.Context, key, field, val string) error {
	if err := h.hCheckKeyForUpdate(ctx, key, field); err != nil {
		return err
	}
	fkey := genHashFieldKey(key, field)
	return h._db.Set(ctx, fkey, codec.Encode(val).SavedData())
}

func (h *_hashImpl) HGet(ctx context.Context, key, field string) (string, error) {

	if err := h.hCheckKey(ctx, []byte(key)); err != nil {
		return "", err
	}
	fkey := genHashFieldKey(key, field)
	data, err := h._db.Get(ctx, fkey)
	if err != nil {
		return "", err
	}
	v := codec.Decode(data)
	if err := h.checkExpire(ctx, key, v.ExpireAt()); err != nil {
		return "", err
	}
	return v.String(), nil
}
func (h *_hashImpl) checkExpire(ctx context.Context, key string, expireAt uint64) error {
	var unixNow = uint64(time.Now().Unix())
	if expireAt != 0 && expireAt <= unixNow {
		go func(ctx context.Context) {
			defer recover()
			keys, _ := h.hGetAllKeys(ctx, key)
			for _, k := range keys {
				_ = h._db.Delete(ctx, k)
			}
			_ = h._db.Delete(ctx, []byte(key))
		}(context.Background())
		return ErrNotfound
	}
	return nil
}

func (h *_hashImpl) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	keys, err := h.hGetAllKeys(ctx, key)
	if err != nil {
		return nil, err
	}
	var m = make(map[string]string, len(key))
	var newKeys = make([]string, 0, len(keys))
	for _, key := range keys {
		data, err := h._db.Get(ctx, key)
		if err == ErrNotfound {
			continue
		}
		if err != nil {
			return nil, err
		}
		m[string(key)] = codec.Decode(data).String()
		newKeys = append(newKeys, string(key))
	}
	if len(newKeys) < len(keys) {
		_ = h._db.Set(ctx, []byte(key), genFields(key, newKeys))
	}
	return m, nil
}

func (h *_hashImpl) HDel(ctx context.Context, key string, field ...string) error {
	keys, err := h.hGetAllKeys(ctx, key)
	if err != nil {
		return err
	}
	var waitDeletes = make(map[string]struct{}, len(keys))
	for _, f := range field {
		waitDeletes[string(genHashFieldKey(key, f))] = struct{}{}
	}
	var newKeys = make([]string, 0, len(keys))
	for _, key := range keys {
		if _, ok := waitDeletes[string(key)]; ok {
			_ = h._db.Delete(ctx, key)
			continue
		}
		newKeys = append(newKeys, string(key))
	}
	if len(newKeys) < len(keys) {
		_ = h._db.Set(ctx, []byte(key), genFields(key, newKeys))
	}
	return nil
}
