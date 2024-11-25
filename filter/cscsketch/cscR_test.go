package cscsketch

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCSCRConstruction(t *testing.T) {
	cscr := NewCSCR(10, 8, 4, 10, 10, 2)
	fmt.Printf("csc = %v\n", cscr)
}

// 测试单个 kv 的插入
func TestCSCRSingleInsertion(t *testing.T) {
	cscr := NewCSCR(10, 8, 4, 10, 10, 2)
	cscr.Add("hello", "file1")
	assert.Equal(t, cscr.Get("hello"), []string{"file1"})
	assert.Equal(t, cscr.Get("world"), make([]string, 0))
}

// 测试多个 kv 的插入，其中一个 key 只会存在于一个 file （不考虑哈希碰撞）
func TestCSCRBatchInsertion(t *testing.T) {
	cscR := NewCSCR(10, 8, 4, 10, 3, 2)
	files := []string{"file1", "file2", "file3", "file4", "file5", "file6", "file7", "file8", "file9", "file10"}
	r := randomSeed()
	keys := generateRandomStrings(8, 100, r)
	pairs := make(map[string]string, len(keys))
	for _, key := range keys {
		pairs[key] = files[rand.Int31n(int32(len(files)))]
	}
	for key, file := range pairs {
		cscR.Add(key, file)
	}
	// 测试 file 是否包含在查询的结果里
	for key, file := range pairs {
		fmt.Printf("key: %v, file: %v, result: %v\n", key, file, cscR.Get(key))
		assert.Equal(t, contains(cscR.Get(key), file), true)
	}
}

// 测试多个 kv 的插入，其中一个 key 存在于多个 file
func TestCSCRBatchInsertion2(t *testing.T) {
	cscr := NewCSCR(10, 8, 4, 10, 5, 2)
	r := randomSeed()
	keys := generateRandomStrings(8, 5, r)
	files := []string{"file1", "file2", "file3", "file4", "file5", "file6", "file7", "file8", "file9", "file10"}
	kvs := make(map[string][]string, len(keys))
	for _, key := range keys {
		for i := 0; i < 2; i++ {
			file := files[rand.Int31n(int32(len(files)))]
			cscr.Add(key, file)
			kvs[key] = append(kvs[key], file)
		}
	}
	for key, files := range kvs {
		assert.Equal(t, containsAll(cscr.Get(key), files), true)
		fmt.Printf("key: %v, files: %v, result1: %v, single_csc_result: %v, single_csc_result: %v\n", key, files, cscr.Get(key), cscr.GetCSC(0).Get(key), cscr.GetCSC(1).Get(key))
	}
}

func TestCSCSize(t *testing.T) {
	csc1 := NewCSC(10, 8, 4, 10, 5)
	// log_2(512) + 1 = 10
	csc2 := NewCSCWithEstimation(512, 8, 4, 10, 5)
	assert.Equal(t, csc1.GetBucketNum(), csc2.GetBucketNum())
}
