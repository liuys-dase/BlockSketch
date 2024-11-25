package basicfilter

import (
	"fmt"
	"log"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCuckooFilterConstruction(t *testing.T) {
	cf := NewCuckooFilter(10, 8, 4, 10)
	fmt.Println(cf.BucketSize())
	fmt.Println(cf.SlotSize())
}

func TestEstimateBucketPow(t *testing.T) {
	log.Printf("estimateBucketPow(): %d\n", EstimateBucketPow(29, 4, 0))
}

func TestCuckooFilterAdd(t *testing.T) {
	cf := NewCuckooFilter(10, 8, 4, 10)
	cf.Add("hello")
	cf.Add("world")
	assert.Equal(t, sumNotEmptySlot(cf), 2)
	assert.Equal(t, cf.Get("hello"), true)
	assert.Equal(t, cf.Get("world"), true)
}

// 生成小于 fingerprintNum 的键，测试同一个 Bucket 内的冲突情况
func TestSameBucketConflict(t *testing.T) {
	cf := NewCuckooFilter(4, 8, 4, 10)
	keys := generateConflictKeys(cf, 2, 8)
	fmt.Printf("conflict keys: %v\n", keys)
	for _, key := range keys {
		cf.Add(key)
	}
	for _, key := range keys {
		assert.Equal(t, cf.Get(key), true)
	}
	sumNotEmptyBucket(cf)
}

// 生成大于 fingerprintNum 的键，测试跨 Bucket 内的冲突情况
func TestCrossBucketConflict(t *testing.T) {
	cf := NewCuckooFilter(4, 8, 4, 10)
	keys := generateConflictKeys(cf, 5, 8)
	fmt.Printf("conflict keys: %v\n", keys)
	for _, key := range keys {
		cf.Add(key)
	}
	for _, key := range keys {
		assert.Equal(t, cf.Get(key), true)
	}
	sumNotEmptyBucket(cf)
}

func sumNotEmptySlot(cf *CuckooFilter) int {
	emptySlot := string(make([]byte, cf.FingerprintSize()))
	sum := 0
	for _, bucket := range cf.Buckets() {
		for _, fingerprint := range bucket.GetFingerprints() {
			if string(fingerprint) != emptySlot {
				sum++
			}
		}
	}
	return sum
}

func sumNotEmptyBucket(cf *CuckooFilter) {
	emptySlot := string(make([]byte, cf.FingerprintSize()))
	for i, bucket := range cf.Buckets() {
		count := 0
		for _, fingerprint := range bucket.GetFingerprints() {
			if string(fingerprint) != emptySlot {
				count++
			}
		}
		if count > 0 {
			fmt.Printf("bucket %d has %d none-empty slot\n", i, count)
		}
	}
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

func generateConflictKeys(cf *CuckooFilter, count int, k int) []string {
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
