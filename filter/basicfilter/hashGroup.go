package basicfilter

import "github.com/cespare/xxhash/v2"

type BFHashGroup struct {
	HashFunc  []*xxhash.Digest // 保存哈希函数
	Key       string           // 保存 HashFunc 的计算对象
	HashValue []uint64         // 保存 HashFunc 的计算结果
	Seeds     []uint64
}

func NewBFHashGroup(num int) *BFHashGroup {
	hashFunc := make([]*xxhash.Digest, num)
	seeds := make([]uint64, num)
	for i := 0; i < num; i++ {
		hashFunc[i] = xxhash.NewWithSeed(uint64(i))
		seeds[i] = uint64(i)
	}
	return &BFHashGroup{
		HashFunc: hashFunc,
		Seeds:    seeds,
	}
}

func (hashGroup *BFHashGroup) ResetWithSeed() {
	for i, hashFunc := range hashGroup.HashFunc {
		hashFunc.ResetWithSeed(hashGroup.Seeds[i])
	}
}

func (hashGroup *BFHashGroup) Write(key string) []uint64 {
	if hashGroup.Key == key {
		return hashGroup.HashValue
	} else {
		hashGroup.Key = key
		hashGroup.ResetWithSeed()
		hashGroup.HashValue = make([]uint64, len(hashGroup.HashFunc))
		key_byte := []byte(key)
		for i, hashFunc := range hashGroup.HashFunc {
			hashFunc.Write(key_byte)
			hashGroup.HashValue[i] = hashFunc.Sum64()
		}
		return hashGroup.HashValue
	}
}
