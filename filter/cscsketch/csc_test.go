package cscsketch

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"testing"

	"github.com/liuys-dase/csc-tree/filter/basicfilter"
	"github.com/stretchr/testify/assert"
)

func TestUtilizationRate(t *testing.T) {
	elementNum := 13000
	partitionNum := 128
	fileNum := 4
	csc := NewCSCWithEstimation(elementNum, 8, 4, 30, partitionNum)
	log.Printf("estimated slots number of csc: %v\n", len(csc.Buckets)*csc.SlotNum)
	addrs := generateRandomStrings(20, elementNum, randomSeed())
	files := generateRandomStrings(20, fileNum, randomSeed())
	retry := true
	for retry {
		for i := 0; i < elementNum; i++ {
			if !csc.Add(addrs[i], files[i%fileNum]) {
				retry = true
				csc.Double()
				log.Printf("===== full =====")
				// break
				return
			} else {
				retry = false
			}
		}
	}
	assert.Equal(t, retry, false)
	log.Printf("bit size of csc: %v\n", csc.GetBitSize())
	log.Printf("slots number of csc: %v\n", len(csc.Buckets)*csc.SlotNum)
	log.Printf("csc utilization rate: %v\n", csc.GetUtilizationRate())
}

func TestCSCConstruction(t *testing.T) {
	csc := NewCSC(10, 8, 4, 10, 10)
	fmt.Printf("csc = %v\n", csc)
}

func TestCscAltIndex(t *testing.T) {
	for i := 0; i < 1000; i++ {
		csc := NewCSC(3, 8, 4, 10, 10)
		target := "0x5d720fd6b04a0cde7d1684832ab55e8a6552bc49"
		fp := csc.Fingerprint(target)
		fp_byte := csc.Uint64ToBytes(fp)
		index := csc.GetIndex(target, "18000002")
		alt_index := csc.GetAltIndex(index, fp_byte)
		alt_alt_index := csc.GetAltIndex(alt_index, fp_byte)
		assert.Equal(t, index, alt_alt_index)
	}
}

func TestCscSizeEstimation(t *testing.T) {
	csc := NewCSCWithEstimation(8444, 8, 4, 10, 10)
	fmt.Printf("bucket num of csc: %v\n", csc.GetBucketNum())
	fmt.Printf("fingerprint num of csc: %v\n", csc.GetFingerprintNum())
	fmt.Printf("bit size of csc: %v\n", csc.GetBitSize())
}

func TestCscGetIndex(t *testing.T) {
	csc := NewCSC(10, 8, 4, 10, 10)
	fp := csc.Fingerprint("hello")
	fp_byte := csc.Uint64ToBytes(fp)
	index := csc.GetIndex("hello", "file1")
	altIndex := csc.GetAltIndex(index, fp_byte)
	altAltIndex := csc.GetAltIndex(altIndex, fp_byte)
	fmt.Printf("fp: %v, index: %v, altIndex: %v, altAltIndex: %v\n", fp, index, altIndex, altAltIndex)
}

// 测试单个 kv 的插入
func TestCSCSingleInsertion(t *testing.T) {
	csc := NewCSC(10, 8, 4, 10, 10)
	csc.Add("hello", "file1")
	// fmt.Printf("key: hello, anchor: %d, offset: %d, index: %d\n", csc.Anchor("hello"), csc.Offset("file1"), csc.GetIndex("hello", "file1"))
	assert.Equal(t, csc.Get("hello"), []string{"file1"})
	assert.Equal(t, csc.Get("world"), []string(nil))
}

// 测试多个 kv 的插入，其中一个 key 只会存在于一个 file （不考虑哈希碰撞）
func TestCSCBatchInsertion(t *testing.T) {
	csc := NewCSC(10, 8, 4, 10, 3)
	files := []string{"file1", "file2", "file3", "file4", "file5", "file6", "file7", "file8", "file9", "file10"}
	r := randomSeed()
	keys := generateRandomStrings(8, 100, r)
	pairs := make(map[string]string, len(keys))
	for _, key := range keys {
		pairs[key] = files[rand.Int31n(int32(len(files)))]
	}
	for key, file := range pairs {
		csc.Add(key, file)
	}
	// 测试 file 是否包含在查询的结果里
	for key, file := range pairs {
		assert.Equal(t, contains(csc.Get(key), file), true)
	}
}

// 测试多个 kv 的插入，其中一个 key 存在于多个 file
func TestCSCBatchInsertion2(t *testing.T) {
	csc := NewCSC(10, 8, 4, 10, 3)
	r := randomSeed()
	keys := generateRandomStrings(8, 5, r)
	files := []string{"file1", "file2", "file3", "file4", "file5", "file6", "file7", "file8", "file9", "file10"}
	kvs := make(map[string][]string, len(keys))
	for _, key := range keys {
		for i := 0; i < 3; i++ {
			file := files[rand.Int31n(int32(len(files)))]
			csc.Add(key, file)
			kvs[key] = append(kvs[key], file)
		}
	}
	for key, files := range kvs {
		assert.Equal(t, containsAll(csc.Get(key), files), true)
		fmt.Printf("key: %v, files: %v, result: %v\n", key, files, csc.Get(key))
	}

}

func TestCSCMaxAttempts(t *testing.T) {
	elementNum := 489
	loopTime := 100
	for i := 0; i < loopTime; i++ {
		csc := NewCSCWithEstimation(elementNum, 8, 4, 20, 3)
		r := randomSeed()
		keys := generateRandomStrings(8, elementNum, r)
		files := generateRandomStrings(8, elementNum, r)
		fmt.Printf("keys: %v\n", keys)
		for i := range keys {
			csc.Add(keys[i], files[i])
		}
	}
}

func contains(arr []string, key string) bool {
	for _, k := range arr {
		if k == key {
			return true
		}
	}
	return false
}

func containsAll(arr []string, keys []string) bool {
	for _, key := range keys {
		if !contains(arr, key) {
			return false
		}
	}
	return true
}

func randomSeed() *rand.Rand {
	return rand.New(rand.NewSource(rand.Int63()))
}

func generateRandomString(k int, r *rand.Rand) string {
	chars := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, k)
	for i := 0; i < k; i++ {
		result[i] = chars[r.Intn(len(chars))]
	}
	return string(result)
}

func generateRandomStrings(k int, num int, r *rand.Rand) []string {
	strings := make([]string, 0)
	for i := 0; i < num; i++ {
		strings = append(strings, generateRandomString(k, r))
	}
	return strings
}

func generateConflictKeys(cf *basicfilter.CuckooFilter, count int, k int) []string {
	keys := make([]string, count)
	r := randomSeed()
	keys[0] = generateRandomString(k, r)
	index := cf.GetIndex(keys[0])
	// 输出 keys[0] 和 index
	// fmt.Printf("keys[0]: %v, index: %d\n", keys[0], index)
	for i := 1; i < count; i++ {
		for {
			ck := generateRandomString(k, r)
			if cf.GetIndex(ck) == index {
				keys[i] = ck
				break
			}
		}
	}
	return keys
}

func TestDivide(t *testing.T) {
	fmt.Printf("test: %v", math.Ceil(float64(25)/float64(8)))
}
