package basicfilter

import (
	"crypto/md5"
	"encoding/hex"
	"log"
	"math"
	"math/rand"
)

type Bucket struct {
	Fingerprints [][]byte
}

func (b *Bucket) GetFingerprints() [][]byte {
	return b.Fingerprints
}

type CuckooFilter struct {
	bucketPow       int
	numBuckets      int
	buckets         []*Bucket
	mask            int
	fingerprintSize int
	fingerprintNum  int
	maxKickAttempts int
}

// NewCuckooFilter 创建一个新的布谷鸟哈希表
func NewCuckooFilter(bucketPow int, fingerprintSize int, fingerprintNum int, maxKickAttempts int) *CuckooFilter {
	numBuckets := 1 << bucketPow
	buckets := make([]*Bucket, numBuckets)
	for i := range buckets {
		buckets[i] = &Bucket{
			Fingerprints: make([][]byte, fingerprintNum),
		}
		for j := range buckets[i].Fingerprints {
			buckets[i].Fingerprints[j] = make([]byte, fingerprintSize)
		}
	}
	return &CuckooFilter{
		bucketPow:       bucketPow,
		numBuckets:      numBuckets,
		buckets:         buckets,
		mask:            (1 << bucketPow) - 1,
		fingerprintSize: fingerprintSize,
		fingerprintNum:  fingerprintNum,
		maxKickAttempts: maxKickAttempts,
	}
}

func NewCuckooFilterWithEstimation(elementNum int, fingerprintSize int, fingerprintNum int, maxKickAttempts int) *CuckooFilter {
	bucketPow := EstimateBucketPow(elementNum, fingerprintNum, 0)
	return NewCuckooFilter(bucketPow, fingerprintSize, fingerprintNum, maxKickAttempts)
}

func (cf *CuckooFilter) hash(item string) string {
	h := md5.New()
	h.Write([]byte(item))
	return hex.EncodeToString(h.Sum(nil))
}

func (cf *CuckooFilter) fingerprint(item string) []byte {
	hash := cf.hash(item)
	return []byte(hash[:cf.fingerprintSize])
}

func (cf *CuckooFilter) GetIndex(item string) int {
	hash, _ := hex.DecodeString(cf.hash(item))
	var hashValue int
	for _, b := range hash {
		hashValue = (hashValue << 8) | int(b)
	}
	return hashValue & cf.mask
}

func (cf *CuckooFilter) getAltIndex(index int, fingerprint []byte) int {
	fingerprintHash, _ := hex.DecodeString(cf.hash(string(fingerprint)))
	var fingerprintValue int
	for _, b := range fingerprintHash {
		fingerprintValue = (fingerprintValue << 8) | int(b)
	}
	return (index ^ fingerprintValue) & cf.mask
}

// 将 cf 的第 `bucket_index` 个 bucket 的第 `fingerprint_index` 个 fingerprint 和 bitmap 替换为传入的 fingerprint 和 bitmap
func (cf *CuckooFilter) swap(bucket_index int, fingerprint_index int, fingerprint []byte) []byte {
	tmpFingerprint := cf.buckets[bucket_index].Fingerprints[fingerprint_index]
	cf.buckets[bucket_index].Fingerprints[fingerprint_index] = fingerprint
	return tmpFingerprint
}

// 判断 cf 的第 `bucket_index` 个 bucket 是否包含传入的 fingerprint
func (cf *CuckooFilter) contains(bucket_index int, fingerprint []byte) bool {
	for _, f := range cf.buckets[bucket_index].Fingerprints {
		if string(f) == string(fingerprint) {
			return true
		}
	}
	return false
}

// 判断 cf 的第 `bucket_index` 个 bucket 是否有空位，有空位返回空位的 index，没有返回 -1
func (cf *CuckooFilter) hasEmpty(bucket_index int) int {
	for i, f := range cf.buckets[bucket_index].Fingerprints {
		if string(f) == string(make([]byte, cf.fingerprintSize)) {
			return i
		}
	}
	return -1
}

func (cf *CuckooFilter) Add(item string) bool {
	fingerprint := cf.fingerprint(item)

	bucketIndex := cf.GetIndex(item)
	altBucketIndex := cf.getAltIndex(bucketIndex, fingerprint)

	if cf.contains(bucketIndex, fingerprint) {
		return true
	}

	if cf.contains(altBucketIndex, fingerprint) {
		return true
	}

	emptyFlag := cf.hasEmpty(bucketIndex)
	altEmptyFlag := cf.hasEmpty(altBucketIndex)

	// 若两个 bucket 都有空位，则随机选择一个 bucket
	if emptyFlag != -1 && altEmptyFlag != -1 {
		if rand.Intn(2) == 1 {
			cf.buckets[bucketIndex].Fingerprints[emptyFlag] = fingerprint
		} else {
			cf.buckets[altBucketIndex].Fingerprints[altEmptyFlag] = fingerprint
		}
		return true
	}

	if emptyFlag != -1 {
		cf.buckets[bucketIndex].Fingerprints[emptyFlag] = fingerprint
		return true
	}

	if altEmptyFlag != -1 {
		cf.buckets[altBucketIndex].Fingerprints[altEmptyFlag] = fingerprint
		return true
	}

	selectBucketIndex := bucketIndex
	if rand.Intn(2) == 1 {
		selectBucketIndex = altBucketIndex
	}

	tmpFingerprint := cf.swap(selectBucketIndex, rand.Intn(cf.fingerprintNum), fingerprint)
	kickCount := 1
	altBucketIndex = cf.getAltIndex(selectBucketIndex, tmpFingerprint)

	emptyFlag = cf.hasEmpty(selectBucketIndex)
	for emptyFlag == -1 {
		tmpFingerprint = cf.swap(altBucketIndex, rand.Intn(cf.fingerprintNum), tmpFingerprint)
		altBucketIndex = cf.getAltIndex(altBucketIndex, tmpFingerprint)
		kickCount++
		if kickCount > cf.maxKickAttempts {
			panic("max kick attempts reached")
		}
		emptyFlag = cf.hasEmpty(selectBucketIndex)
	}

	cf.buckets[altBucketIndex].Fingerprints[emptyFlag] = tmpFingerprint
	log.Printf("===== add finish =====")
	return true
}

func (cf *CuckooFilter) Get(item string) bool {
	fingerprint := cf.fingerprint(item)
	index := cf.GetIndex(item)
	altIndex := cf.getAltIndex(index, fingerprint)
	if cf.contains(index, fingerprint) {
		return true
	}
	if cf.contains(altIndex, fingerprint) {
		return true
	}
	return false
}

func EstimateBucketPow(elementNum int, slotNum int, partitionNum int) int {
	// return bucketPow
	minBucketNum := int(math.Ceil(float64(elementNum) / float64(slotNum)))
	// bucketPow ：log_2{minBucketNum}向上取整
	bucketPow := int(math.Ceil(math.Log2(float64(minBucketNum))))
	partitionPow := int(math.Ceil(math.Log2(float64(partitionNum))))
	// if bucketPow <= partitionPow {
	// 	return partitionPow + 1
	// }
	bucketNum := 1 << bucketPow
	if bucketNum <= partitionNum {
		// return partitionPow + 1
		return partitionPow
	}
	// 输出 elementNum, bucketPow, partitionPow
	// log.Printf("elementNum: %d, bucketPow: %d, partitionPow: %d\n", elementNum, bucketPow, partitionPow)
	return bucketPow
}

func (cf *CuckooFilter) BucketSize() int {
	return cf.numBuckets
}

func (cf *CuckooFilter) SlotSize() int {
	return cf.fingerprintNum
}

func (cf *CuckooFilter) FingerprintSize() int {
	return cf.fingerprintSize
}

func (cf *CuckooFilter) Buckets() []*Bucket {
	return cf.buckets
}
