package basicfilter

import (
	"math"
	"math/rand"
	"time"

	"github.com/cespare/xxhash/v2"
)

type BloomFilter struct {
	BitArray []bool  // 布尔数组，用于表示元素是否存在
	K        int     // 哈希函数的数量
	M        int     // 布尔数组的长度
	Fpr      float64 // 误判率
	HashFunc []*xxhash.Digest
	Seeds    []uint64
}

// 输出 m
func (bf *BloomFilter) Size() int {
	return bf.M
}

func NewBloomFilter(n int, fpr float64, k int) *BloomFilter {
	if n == 0 {
		return NewEmptyBloomFilter()
	}
	m := FindOptimalM(n, fpr, k)
	hashFunc := make([]*xxhash.Digest, k)
	seeds := make([]uint64, k)
	rng := rand.New(rand.NewSource(time.Now().UnixNano())) // 创建一个独立的随机数生成器
	for i := 0; i < k; i++ {
		seed := rng.Uint64() // 生成随机种子
		seeds[i] = seed
		hashFunc[i] = xxhash.NewWithSeed(seed)
		// 使用不同的种子生成不同的哈希函数
		// hashFunc[i] = xxhash.NewWithSeed(uint64(i))
	}
	return &BloomFilter{
		BitArray: make([]bool, m),
		K:        k,
		M:        m,
		Fpr:      fpr,
		HashFunc: hashFunc,
		Seeds:    seeds,
	}
}

func NewBloomFilterWithHashGroup(n int, fpr float64, k int, hashGroup *BFHashGroup) *BloomFilter {
	if n == 0 {
		return NewEmptyBloomFilter()
	}
	m := FindOptimalM(n, fpr, k)
	return &BloomFilter{
		BitArray: make([]bool, m),
		K:        k,
		M:        m,
		Fpr:      fpr,
		HashFunc: hashGroup.HashFunc,
		Seeds:    hashGroup.Seeds,
	}
}

// 生成一个空的布隆过滤器
func NewEmptyBloomFilter() *BloomFilter {
	return &BloomFilter{
		BitArray: make([]bool, 0),
		K:        0,
		M:        0,
		Fpr:      0,
		HashFunc: make([]*xxhash.Digest, 0),
	}
}

func (bf *BloomFilter) IsEmpty() bool {
	return bf.M == 0
}

func (bf *BloomFilter) Add(item string) {
	for i, hashFunc := range bf.HashFunc {
		// !!!
		// hashFunc.ResetWithSeed(uint64(i))
		hashFunc.ResetWithSeed(bf.Seeds[i])
		hashFunc.Write([]byte(item))
		index := hashFunc.Sum64() % uint64(bf.M)
		bf.BitArray[index] = true
	}
}

func (bf *BloomFilter) BatchAdd(items []string) {
	if bf.IsEmpty() {
		return
	}
	for _, item := range items {
		bf.Add(item)
	}
}

func (bf *BloomFilter) Get(item string) bool {
	if bf.IsEmpty() {
		return false
	}
	for i, hashFunc := range bf.HashFunc {
		// !!!
		// hashFunc.ResetWithSeed(uint64(i))
		hashFunc.ResetWithSeed(bf.Seeds[i])
		hashFunc.Write([]byte(item))
		index := hashFunc.Sum64() % uint64(bf.M)
		// log.Printf("index: %d", index)
		if !bf.BitArray[index] {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) GetWithHashGroup(item string, hashGroup *BFHashGroup) bool {
	if bf.IsEmpty() {
		return false
	}
	// 如果 item 与上一次的 key 相同，则直接返回上一次的结果
	// 如果不同，则重新计算哈希值
	values := hashGroup.Write(item)
	for i := 0; i < len(bf.HashFunc); i++ {
		index := values[i] % uint64(bf.M)
		if !bf.BitArray[index] {
			return false
		}
	}
	return true
}

// 获取布隆过滤器的大小（实际返回的字节）
func (bf *BloomFilter) GetBitSize() int {
	return bf.M / 8
}

func FindOptimalM(n int, fpr float64, k int) int {
	m := n
	for calculateFPR(k, n, m) > fpr {
		m++
	}
	return m
}

func calculateFPR(k, n, m int) float64 {
	exponent := float64(-k*n) / float64(m)
	return math.Pow(1-math.Exp(exponent), float64(k))
}
