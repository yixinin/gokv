package gokv

import (
	"context"
	"strings"

	"github.com/yixinin/gokv/storage"
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
	return fields
}
func genFields(fields []string) []byte {
	s := []byte(strings.Join(fields, ","))
	var data = make([]byte, 2, 2+len(s))
	data[0] = 'h'
	data[1] = ':'
	data = append(data, s...)
	return data
}

type hashImpl struct {
	db storage.Storage
}

func (h *hashImpl) hCheckKey(ctx context.Context, key []byte) error {
	hkval, err := h.db.Get(ctx, key)
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

func (h *hashImpl) hCheckKeyForUpdate(ctx context.Context, key, field string) error {
	var bKey = []byte(key)
	hkval, err := h.db.Get(ctx, bKey)
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
		fields := readFields(hkval[2:])
		for _, f := range fields {
			if f == field {
				return nil
			}
		}
		fields = append(fields, field)
		return h.db.Set(ctx, bKey, genFields(fields))
	case ErrNotfound:
		val := make([]byte, len(key)+len(field)+1)
		copy(val[:len(key)], key)
		val[len(key)] = HashKeySplitC
		copy(val[len(key)+1:], field)
		return h.db.Set(ctx, bKey, val)
	default:
		return err
	}
}

func genHashFieldKey(key, field string) []byte {
	fkey := make([]byte, 2+len(key)+len(field)+1)
	fkey[0] = 'h'
	fkey[1] = ':'
	copy(fkey[:len(key)], key)
	fkey[len(key)] = SplitC
	copy(fkey[len(key)+1:], field)
	return fkey
}
func (h *hashImpl) HSet(ctx context.Context, key, field, val string) error {
	if err := h.hCheckKeyForUpdate(ctx, key, field); err != nil {
		return err
	}
	fkey := genHashFieldKey(key, field)
	return h.db.Set(ctx, fkey, storage.String2Bytes(val))
}

func (h *hashImpl) HGet(ctx context.Context, key, field string) (string, error) {
	if err := h.hCheckKey(ctx, []byte(key)); err != nil {
		return "", err
	}
	fkey := genHashFieldKey(key, field)
	data, err := h.db.Get(ctx, fkey)
	if err != nil {
		return "", err
	}
	return storage.Bytes2String(data), nil
}
