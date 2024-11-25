package cscsketch

import (
	"math"
	"math/rand"

	"github.com/cespare/xxhash/v2"
	"github.com/liuys-dase/csc-tree/filter/basicfilter"
)

type CSC struct {
	// CuckooFilter 的配置信息
	BucketPow              int
	NumBuckets             int
	Buckets                []*basicfilter.Bucket
	Mask                   int
	FingerprintSize        int
	FingerprintByteArrSize int
	SlotNum                int
	MaxKickAttempts        int
	// CSC 额外的配置信息
	SeedAnchor   uint64
	SeedOffset   uint64
	PartitionNum int
	Partitions   *GlobalPartition
	// 用于统计利用率
	Utilization_count int
}

// 所有 csc 的变种都基于这个构造方法，并不会直接调用这个方法，而是调用 NewCSCWithEstimation
func NewCSC(bucketPow int, fingerprintSize int, slotNum int, maxKickAttempts int, partitionNum int) *CSC {
	numBuckets := 1 << bucketPow
	buckets := make([]*basicfilter.Bucket, numBuckets)
	fingerprintByteArrSize := int(math.Ceil(float64(fingerprintSize) / float64(8)))
	for i := range buckets {
		buckets[i] = &basicfilter.Bucket{
			Fingerprints: make([][]byte, slotNum),
		}
		for j := range buckets[i].Fingerprints {
			buckets[i].Fingerprints[j] = make([]byte, fingerprintByteArrSize)
		}
	}
	seedAnchor := rand.Uint64()
	seedOffset := rand.Uint64()
	return &CSC{
		BucketPow:              bucketPow,
		NumBuckets:             numBuckets,
		Buckets:                buckets,
		Mask:                   (1 << bucketPow) - 1,
		FingerprintSize:        fingerprintSize,
		FingerprintByteArrSize: fingerprintByteArrSize,
		SlotNum:                slotNum,
		MaxKickAttempts:        maxKickAttempts,
		SeedAnchor:             seedAnchor,
		SeedOffset:             seedOffset,
		PartitionNum:           partitionNum,
		Partitions:             NewGlobalPartition(partitionNum, seedOffset),
	}
}

// Double 时会清空 Buckets 和 Partitions
func (csc *CSC) Double() {
	csc.BucketPow++
	csc.NumBuckets = 1 << csc.BucketPow
	newBuckets := make([]*basicfilter.Bucket, csc.NumBuckets)
	for i := range newBuckets {
		newBuckets[i] = &basicfilter.Bucket{
			Fingerprints: make([][]byte, csc.SlotNum),
		}
		for j := range newBuckets[i].Fingerprints {
			newBuckets[i].Fingerprints[j] = make([]byte, csc.FingerprintSize)
		}
	}
	csc.Buckets = newBuckets
	csc.Mask = (1 << csc.BucketPow) - 1
	csc.Partitions.Clear()
	csc.Utilization_count = 0
}

func NewCSCWithEstimation(elementNum int, fingerprintSize int, slotNum int, maxKickAttempts int, partitionNum int) *CSC {
	if elementNum == 0 {
		return NewEmptyCSC()
	}
	bucketPow := basicfilter.EstimateBucketPow(elementNum, slotNum, partitionNum)
	return NewCSC(bucketPow, fingerprintSize, slotNum, maxKickAttempts, partitionNum)
}

// 创建一个空的 CSC
func NewEmptyCSC() *CSC {
	return NewCSC(0, 0, 0, 0, 0)
}

// 返回一个两倍大小的 CSC
func (csc *CSC) DoubleSize() *CSC {
	return NewCSC(csc.BucketPow+1, csc.FingerprintSize, csc.SlotNum, csc.MaxKickAttempts, csc.PartitionNum)
}

func (csc *CSC) IsEmpty() bool {
	return csc.NumBuckets == 0
}

// 根据 key 计算 Anchor
func (csc *CSC) Anchor(item string) int {
	h := xxhash.NewWithSeed(csc.SeedAnchor)
	h.Write([]byte(item))
	hashValue := h.Sum64()
	return int(hashValue) & csc.Mask
}

// 根据 key 计算 Offset
func (csc *CSC) Offset(item string) int {
	h := xxhash.NewWithSeed(csc.SeedOffset)
	h.Write([]byte(item))
	hashValue := h.Sum64()
	return int(hashValue % uint64(csc.PartitionNum))
}

func (csc *CSC) Fingerprint(item string) uint64 {
	h := xxhash.New()
	h.Write([]byte(item))
	// return h.Sum(nil)[:csc.FingerprintSize]
	return h.Sum64() >> (64 - csc.FingerprintSize)
}

// 返回一个长度为 length 的 fingerprint
func (csc *CSC) FingerprintWithLength(item string, length int) uint64 {
	h := xxhash.New()
	h.Write([]byte(item))
	return h.Sum64() >> (64 - length)
}

// CSC 的 GetIndex 方法需要接受两个参数，一个是 item，一个是 fileId
func (csc *CSC) GetIndex(item string, fileId string) int {
	anchor := csc.Anchor(item)
	offset := csc.Offset(fileId)
	return (anchor + offset) & csc.Mask
}

func (csc *CSC) GetAltIndex(index int, fingerprint_byte []byte) int {
	// to be optimized
	h := xxhash.NewWithSeed(csc.SeedAnchor)
	h.Write(fingerprint_byte)
	fingerprintHash := h.Sum64()
	return (index ^ int(fingerprintHash)) & csc.Mask
}

// 将 cf 的第 `bucket_index` 个 bucket 的第 `fingerprint_index` 个 fingerprint 和 bitmap 替换为传入的 fingerprint 和 bitmap
func (csc *CSC) swap(bucket_index int, fingerprint_index int, fingerprint []byte) []byte {
	tmpFingerprint := csc.Buckets[bucket_index].Fingerprints[fingerprint_index]
	csc.Buckets[bucket_index].Fingerprints[fingerprint_index] = fingerprint
	return tmpFingerprint
}

// 判断 cf 的第 `bucket_index` 个 bucket 是否包含传入的 fingerprint
func (csc *CSC) contains(bucket_index int, fingerprint_byte []byte) bool {
	for _, f := range csc.Buckets[bucket_index].Fingerprints {
		// !isEmptySlot(f) 用来放置 fp 被计算为 0 的情况，但是会带来假阴
		if string(f) == string(fingerprint_byte) && !isEmptySlot(f) {
			return true
		}
	}
	return false
}

// 判断 cf 的第 `bucket_index` 个 bucket 是否有空位，没有则返回 -1
func (csc *CSC) hasEmpty(bucket_index int) int {
	for i, f := range csc.Buckets[bucket_index].Fingerprints {
		if isEmptySlot(f) {
			return i
		}
	}
	return -1
}

func isEmptySlot(f []byte) bool {
	for _, b := range f {
		if b != 0 {
			return false
		}
	}
	return true
}

func (csc *CSC) Add(item string, fildId string) bool {
	// 添加到 GlobalPartition
	csc.Partitions.Add(fildId)

	fingerprint := csc.Fingerprint(item)
	fingerprint_byte := csc.Uint64ToBytes(fingerprint)

	bucketIndex := csc.GetIndex(item, fildId)
	altBucketIndex := csc.GetAltIndex(bucketIndex, fingerprint_byte)

	if csc.contains(bucketIndex, fingerprint_byte) {
		return true
	}

	if csc.contains(altBucketIndex, fingerprint_byte) {
		return true
	}

	emptyFlag := csc.hasEmpty(bucketIndex)
	altEmptyFlag := csc.hasEmpty(altBucketIndex)

	// 若两个 bucket 都有空位，则随机选择一个 bucket
	if emptyFlag != -1 && altEmptyFlag != -1 {
		// if rand.Intn(2) == 1 {
		// 	csc.Buckets[bucketIndex].Fingerprints[emptyFlag] = fingerprint
		// } else {
		// 	csc.Buckets[altBucketIndex].Fingerprints[altEmptyFlag] = fingerprint
		// }
		// 优先填充到 bucketIndex
		csc.Buckets[bucketIndex].Fingerprints[emptyFlag] = fingerprint_byte
		csc.Utilization_count++
		return true
	}

	if emptyFlag != -1 {
		csc.Buckets[bucketIndex].Fingerprints[emptyFlag] = fingerprint_byte
		csc.Utilization_count++
		return true
	}

	if altEmptyFlag != -1 {
		csc.Buckets[altBucketIndex].Fingerprints[altEmptyFlag] = fingerprint_byte
		csc.Utilization_count++
		return true
	}

	// 若两个 bucket 都满了，则需要随机找一个 bucket 中的 slot 进行替换
	selectBucketIndex := bucketIndex
	if rand.Intn(2) == 1 {
		selectBucketIndex = altBucketIndex
	}

	tmpFingerprint := csc.swap(selectBucketIndex, rand.Intn(csc.SlotNum), fingerprint_byte)
	kickCount := 1
	altBucketIndex = csc.GetAltIndex(selectBucketIndex, tmpFingerprint)

	emptyFlag = csc.hasEmpty(altBucketIndex)
	for emptyFlag == -1 {
		tmpFingerprint = csc.swap(altBucketIndex, rand.Intn(csc.SlotNum), tmpFingerprint)
		altBucketIndex = csc.GetAltIndex(altBucketIndex, tmpFingerprint)
		kickCount++
		if kickCount > csc.MaxKickAttempts {
			return false
		}
		emptyFlag = csc.hasEmpty(altBucketIndex)
	}

	csc.Buckets[altBucketIndex].Fingerprints[emptyFlag] = tmpFingerprint
	csc.Utilization_count++
	return true
}

func (cf *CSC) Get(item string) []string {
	fp := cf.Fingerprint(item)
	fp_byte := cf.Uint64ToBytes(fp)
	anchor := cf.Anchor(item)
	result := make([]string, 0)
	for offset := 0; offset < cf.PartitionNum; offset++ {
		index := (anchor + offset) & cf.Mask
		altIndex := cf.GetAltIndex(index, fp_byte)
		if cf.contains(index, fp_byte) || cf.contains(altIndex, fp_byte) {
			result = append(result, cf.Partitions.Get(offset)...)
		}
	}
	return result
}

func (cf *CSC) GetWithCache(item string, cache *CSCCache) []string {
	var fp_byte []byte
	if ok, tmp_fp_byte := cache.getFingerprintByte(); ok {
		fp_byte = tmp_fp_byte
	} else {
		fp := cf.Fingerprint(item)
		fp_byte = cf.Uint64ToBytes(fp)
		cache.setFingerprintHash(fp_byte)
	}
	anchor := cf.AnchorWithCache(item, cache)
	result := make([]string, 0)
	for offset := 0; offset < cf.PartitionNum; offset++ {
		index := (anchor + offset) & cf.Mask
		altIndex := cf.GetAltIndexWithCache(index, fp_byte, cache)
		if cf.contains(index, fp_byte) || cf.contains(altIndex, fp_byte) {
			result = append(result, cf.Partitions.Get(offset)...)
		}
	}
	return result
	// fp := cf.Fingerprint(item)
	// fp_byte := cf.Uint64ToBytes(fp)
	// anchor := cf.Anchor(item)
	// result := make([]string, 0)
	// for offset := 0; offset < cf.PartitionNum; offset++ {
	// 	index := (anchor + offset) & cf.Mask
	// 	altIndex := cf.GetAltIndex(index, fp_byte)
	// 	if cf.contains(index, fp_byte) || cf.contains(altIndex, fp_byte) {
	// 		result = append(result, cf.Partitions.Get(offset)...)
	// 	}
	// }
	// return result
}

func (csc *CSC) AnchorWithCache(item string, cache *CSCCache) int {
	if ok, hashItem := cache.getHashItem(); ok {
		return int(hashItem) & csc.Mask
	} else {
		h := xxhash.NewWithSeed(csc.SeedAnchor)
		h.Write([]byte(item))
		hashValue := h.Sum64()
		cache.setHashItem(hashValue)
		return int(hashValue) & csc.Mask
	}
	// h := xxhash.NewWithSeed(csc.SeedAnchor)
	// h.Write([]byte(item))
	// hashValue := h.Sum64()
	// return int(hashValue) & csc.Mask
}

func (csc *CSC) GetAltIndexWithCache(index int, fingerprint_byte []byte, cache *CSCCache) int {
	// to be optimized
	if ok, fingerprintHash := cache.getFingerprintByteHash(); ok {
		return (index ^ int(fingerprintHash)) & csc.Mask
	} else {
		h := xxhash.NewWithSeed(csc.SeedAnchor)
		h.Write(fingerprint_byte)
		fingerprintHash := h.Sum64()
		cache.setFingerprintByteHash(fingerprintHash)
		return (index ^ int(fingerprintHash)) & csc.Mask
	}
	// h := xxhash.NewWithSeed(csc.SeedAnchor)
	// h.Write(fingerprint_byte)
	// fingerprintHash := h.Sum64()
	// return (index ^ int(fingerprintHash)) & csc.Mask
}

// 用于创建带有 CSCCache 的 CSC，统一各个 CSC 中的某些参数，如 seedAnchor 和 seedOffset
func NewCSCWithCache(bucketPow int, fingerprintSize int, slotNum int, maxKickAttempts int, partitionNum int, cscCache *CSCCache) *CSC {
	numBuckets := 1 << bucketPow
	buckets := make([]*basicfilter.Bucket, numBuckets)
	fingerprintByteArrSize := int(math.Ceil(float64(fingerprintSize) / float64(8)))
	for i := range buckets {
		buckets[i] = &basicfilter.Bucket{
			Fingerprints: make([][]byte, slotNum),
		}
		for j := range buckets[i].Fingerprints {
			buckets[i].Fingerprints[j] = make([]byte, fingerprintByteArrSize)
		}
	}
	seedAnchor := cscCache.SeedAnchor
	seedOffset := cscCache.SeedOffset
	// seedAnchor := rand.Uint64()
	// seedOffset := rand.Uint64()
	return &CSC{
		BucketPow:              bucketPow,
		NumBuckets:             numBuckets,
		Buckets:                buckets,
		Mask:                   (1 << bucketPow) - 1,
		FingerprintSize:        fingerprintSize,
		FingerprintByteArrSize: fingerprintByteArrSize,
		SlotNum:                slotNum,
		MaxKickAttempts:        maxKickAttempts,
		SeedAnchor:             seedAnchor,
		SeedOffset:             seedOffset,
		PartitionNum:           partitionNum,
		Partitions:             NewGlobalPartition(partitionNum, seedOffset),
	}
}

func NewCSCWithEstimationWithCache(elementNum int, fingerprintSize int, slotNum int, maxKickAttempts int, partitionNum int, cache *CSCCache) *CSC {
	if elementNum == 0 {
		return NewEmptyCSC()
	}
	bucketPow := basicfilter.EstimateBucketPow(elementNum, slotNum, partitionNum)
	return NewCSCWithCache(bucketPow, fingerprintSize, slotNum, maxKickAttempts, partitionNum, cache)
}

func (csc *CSC) GetBucketNum() int {
	return csc.NumBuckets
}

func (csc *CSC) GetFingerprintNum() int {
	return csc.SlotNum
}

func (csc *CSC) GetFingerprintSize() int {
	return csc.FingerprintSize
}

func (csc *CSC) GetBuckets() []*basicfilter.Bucket {
	return csc.Buckets
}

// 计算 CSC 的 bit 数量
func (csc *CSC) GetBitSize() int {
	// return csc.NumBuckets * csc.SlotNum * csc.FingerprintByteArrSize
	return (csc.NumBuckets * csc.SlotNum * csc.FingerprintSize) / 8
}

// 计算利用率
func (csc *CSC) GetUtilizationRate() float64 {
	if csc.IsEmpty() {
		return 0
	}
	numerator := float64(csc.Utilization_count)
	denominator := float64(csc.NumBuckets * csc.SlotNum)
	return math.Round(numerator/denominator*100) / 100
}

func (csc *CSC) Uint64ToBytes(value uint64) []byte {
	bytes := make([]byte, csc.FingerprintByteArrSize)
	// 将 value 的低 csc.FingerprintByteArrSize 位直接存入 byte 数组
	for i := 0; i < csc.FingerprintByteArrSize; i++ {
		// 逐字节将 value 存入数组，采用大端序
		bytes[csc.FingerprintByteArrSize-1-i] = byte(value >> (i * 8))
	}
	return bytes
}
