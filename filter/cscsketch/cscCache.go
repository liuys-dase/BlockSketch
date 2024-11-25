package cscsketch

import (
	"math/rand"
)

type CSCCacheList struct {
	CSCCacheList []*CSCCache
}

func NewCSCCacheList(repetitionNum int) *CSCCacheList {
	cscCacheList := make([]*CSCCache, repetitionNum)
	for i := range cscCacheList {
		cscCacheList[i] = NewCSCCache()
	}
	return &CSCCacheList{
		CSCCacheList: cscCacheList,
	}
}

func (cacheList *CSCCacheList) Clear() {
	for _, cache := range cacheList.CSCCacheList {
		cache.Clear()
	}
}

// 记录多个 csccache 可共用的中间计算结果
type CSCCache struct {
	// SeedAnchor 和 SeedOffset 不可变
	SeedAnchor uint64
	SeedOffset uint64
	// 下面的内容随着查询键的变化改变
	FingerprintByte     []byte
	FingerprintByteHash uint64
	HashItem            uint64
}

func NewCSCCache() *CSCCache {
	return &CSCCache{
		SeedAnchor:          rand.Uint64(),
		SeedOffset:          rand.Uint64(),
		FingerprintByte:     nil,
		FingerprintByteHash: 0,
		HashItem:            0,
	}
}

func (cache *CSCCache) Clear() {
	cache.FingerprintByte = nil
	cache.FingerprintByteHash = 0
	cache.HashItem = 0
}

func (cache *CSCCache) getFingerprintByte() (bool, []byte) {
	if cache.FingerprintByte == nil {
		return false, nil
	}
	return true, cache.FingerprintByte
}

func (cache *CSCCache) setFingerprintHash(fingerprint_byte []byte) {
	cache.FingerprintByte = fingerprint_byte
}

func (cache *CSCCache) getFingerprintByteHash() (bool, uint64) {
	if cache.FingerprintByteHash == 0 {
		return false, 0
	}
	return true, cache.FingerprintByteHash
}

func (cache *CSCCache) setFingerprintByteHash(fingerprint_byte_hash uint64) {
	cache.FingerprintByteHash = fingerprint_byte_hash
}

func (cache *CSCCache) getHashItem() (bool, uint64) {
	if cache.HashItem == 0 {
		return false, 0
	}
	return true, cache.HashItem
}

func (cache *CSCCache) setHashItem(hashItem uint64) {
	cache.HashItem = hashItem
}
