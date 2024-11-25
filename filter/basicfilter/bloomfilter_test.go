package basicfilter

import (
	"fmt"
	"log"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/assert"
)

func TestBloomFilterConstruction(t *testing.T) {
	bf_1 := NewBloomFilter(100, 0.01, 7)
	bf_2 := NewBloomFilter(100, 0.001, 7)
	log.Printf("size of bf_1 is %v", bf_1.Size())
	log.Printf("size of bf_2 is %v", bf_2.Size())
}

func TestBloomFilterInsert(t *testing.T) {
	bf := NewBloomFilter(100, 0.01, 7)
	bf.Add("hello")
	bf.Add("world")
}

func TestBloomFilterContains(t *testing.T) {
	bf := NewBloomFilter(100, 0.01, 7)
	bf.Add("hello")
	bf.Add("world")
	assert.Equal(t, bf.Get("hello"), true)
	assert.Equal(t, bf.Get("world"), true)
	assert.Equal(t, bf.Get("foo"), false)
}

// 测试生成多个哈希函数
func TestHashFunc(t *testing.T) {
	hashFund := make([]*xxhash.Digest, 3)
	for i := 0; i < 3; i++ {
		hashFund[i] = xxhash.NewWithSeed(uint64(i))
	}
	// 生成哈希值
	hashFund[0].Write([]byte("world"))
	hashFund[1].Write([]byte("world"))
	hashFund[2].Write([]byte("world"))
	fmt.Printf("Hash value 1 is %v\n", hashFund[0].Sum64())
	fmt.Printf("Hash value 2 is %v\n", hashFund[1].Sum64())
	fmt.Printf("Hash value 3 is %v\n", hashFund[2].Sum64())
	hashFund[0].ResetWithSeed(0)
	hashFund[1].ResetWithSeed(1)
	hashFund[2].ResetWithSeed(2)
	hashFund[0].Write([]byte("world"))
	hashFund[1].Write([]byte("world"))
	hashFund[2].Write([]byte("world"))
	fmt.Printf("Hash value 1 is %v\n", hashFund[0].Sum64())
	fmt.Printf("Hash value 2 is %v\n", hashFund[1].Sum64())
	fmt.Printf("Hash value 3 is %v\n", hashFund[2].Sum64())
}

func TestOptimalM(t *testing.T) {
	n := 946
	fpr := 0.01
	m := FindOptimalM(n, fpr, 7)
	fmt.Println("Optimal m is", m)
}
