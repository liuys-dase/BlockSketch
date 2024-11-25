package basicfilter

import (
	"math/rand"
	"time"

	"github.com/cespare/xxhash/v2"
)

type TwoWayBloomFilter struct {
	BitArray  []bool  // 布尔数组，用于表示元素是否存在
	K         int     // 哈希函数的数量
	M         int     // 布尔数组的长度
	Fpr       float64 // 误判率
	HashFuncA []*xxhash.Digest
	HashFuncB []*xxhash.Digest
	SeedA     []uint64
	SeedB     []uint64
}

func NewTwoWayBloomFilter(elementNum int, fpr float64, hashFuncNum int) *TwoWayBloomFilter {
	if elementNum == 0 {
		return NewEmptyTwoWayBloomFilter()
	}
	m := FindOptimalM(elementNum, fpr, hashFuncNum)

	hashFuncA := make([]*xxhash.Digest, hashFuncNum)
	hashFuncB := make([]*xxhash.Digest, hashFuncNum)
	SeedA := make([]uint64, hashFuncNum)
	SeedB := make([]uint64, hashFuncNum)
	rng := rand.New(rand.NewSource(time.Now().UnixNano())) // 创建一个独立的随机数生成器
	for i := 0; i < hashFuncNum; i++ {
		seedA := rng.Uint64() // 生成随机种子
		seedB := rng.Uint64()
		hashFuncA[i] = xxhash.NewWithSeed(seedA)
		SeedA[i] = seedA
		hashFuncB[i] = xxhash.NewWithSeed(seedB)
		SeedB[i] = seedB
	}
	return &TwoWayBloomFilter{
		BitArray:  make([]bool, m),
		K:         hashFuncNum,
		M:         m,
		Fpr:       fpr,
		HashFuncA: hashFuncA,
		HashFuncB: hashFuncB,
		SeedA:     SeedA,
		SeedB:     SeedB,
	}
}

// 生成一个空的布隆过滤器
func NewEmptyTwoWayBloomFilter() *TwoWayBloomFilter {
	return &TwoWayBloomFilter{
		BitArray:  make([]bool, 0),
		K:         0,
		M:         0,
		Fpr:       0,
		HashFuncA: make([]*xxhash.Digest, 0),
		HashFuncB: make([]*xxhash.Digest, 0),
		SeedA:     make([]uint64, 0),
		SeedB:     make([]uint64, 0),
	}
}

func (bf *TwoWayBloomFilter) IsEmpty() bool {
	return bf.M == 0
}

func (bf *TwoWayBloomFilter) AddLeft(item string) {
	for i, hashFunc := range bf.HashFuncA {
		hashFunc.Write([]byte(item))
		index := hashFunc.Sum64() % uint64(bf.M)
		bf.BitArray[index] = true
		hashFunc.ResetWithSeed(bf.SeedA[i])
	}
}

func (bf *TwoWayBloomFilter) AddRight(item string) {
	for i, hashFunc := range bf.HashFuncB {
		hashFunc.Write([]byte(item))
		index := hashFunc.Sum64() % uint64(bf.M)
		bf.BitArray[index] = true
		hashFunc.ResetWithSeed(bf.SeedB[i])
	}
}

func (bf *TwoWayBloomFilter) GetLeft(item string) bool {
	if bf.IsEmpty() {
		return false
	}
	for i, hashFunc := range bf.HashFuncA {
		hashFunc.Write([]byte(item))
		index := hashFunc.Sum64() % uint64(bf.M)
		hashFunc.ResetWithSeed(bf.SeedA[i])
		if !bf.BitArray[index] {
			return false
		}
	}
	return true
}

func (bf *TwoWayBloomFilter) GetRight(item string) bool {
	if bf.IsEmpty() {
		return false
	}
	for i, hashFunc := range bf.HashFuncB {
		hashFunc.Write([]byte(item))
		index := hashFunc.Sum64() % uint64(bf.M)
		hashFunc.ResetWithSeed(bf.SeedB[i])
		if !bf.BitArray[index] {
			return false
		}
	}
	return true
}

func (bf *TwoWayBloomFilter) GetBitSize() int {
	return bf.M / 8
}
