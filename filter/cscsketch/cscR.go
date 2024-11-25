package cscsketch

import (
	"math"
	"strconv"
)

// csc with R repetitions

type CscrType int

const (
	SKETCH CscrType = iota + 1
	HASHMAP
)

type CSCR struct {
	CType   CscrType
	CSCs    []*CSC
	HashMap map[string][]string
	R       int
}

// HashMap
func NewCSCRWithHashMap() *CSCR {
	return &CSCR{
		CType:   HASHMAP,
		CSCs:    nil,
		HashMap: make(map[string][]string),
		R:       1, // 避免通过 R=0 判断是否为空
	}
}

// Sketch
func NewCSCR(bucketPow int, fingerprintSize int, fingerprintNum int, maxKickAttempts int, partitionNum int, r int) *CSCR {
	cscs := make([]*CSC, r)
	for i := range cscs {
		cscs[i] = NewCSC(bucketPow, fingerprintSize, fingerprintNum, maxKickAttempts, partitionNum)
	}
	return &CSCR{
		CType:   SKETCH,
		CSCs:    cscs,
		HashMap: nil,
		R:       r,
	}
}

// Sketch
func NewCSCRWithEstimation(elementNum int, fingerprintSize int, slotNum int, maxKickAttempts int, partitionNum int, r int) *CSCR {
	cscs := make([]*CSC, r)
	for i := range cscs {
		cscs[i] = NewCSCWithEstimation(elementNum, fingerprintSize, slotNum, maxKickAttempts, partitionNum)
	}
	return &CSCR{
		CType:   SKETCH,
		CSCs:    cscs,
		HashMap: nil,
		R:       r,
	}
}

func NewEmptyCSCR() *CSCR {
	return NewCSCR(0, 0, 0, 0, 0, 0)
}

// 批量添加元素
func (cscr *CSCR) BatchAdd(kvs map[string]int) {
	retry_flag := true
	for retry_flag {
		for key, value := range kvs {
			if !cscr.Add(key, strconv.Itoa(value)) {
				cscr.Double()
				retry_flag = true
				break
			}
			retry_flag = false
		}
	}
}

func (cscr *CSCR) IsEmpty() bool {
	return cscr.R == 0
}

func (cscr *CSCR) Add(item string, fileId string) bool {
	if cscr.CType == SKETCH {
		for i := range cscr.CSCs {
			if !cscr.CSCs[i].Add(item, fileId) {
				return false
			}
		}
		return true
	} else {
		if _, ok := cscr.HashMap[item]; !ok {
			cscr.HashMap[item] = make([]string, 0)
		}
		// 判断是否已经存在
		for _, val := range cscr.HashMap[item] {
			if val == fileId {
				return true
			}
		}
		cscr.HashMap[item] = append(cscr.HashMap[item], fileId)
		return true
	}
}

func (cscr *CSCR) Double() {
	for i := range cscr.CSCs {
		cscr.CSCs[i].Double()
	}
}

func (cscr *CSCR) Get(item string) []string {
	result := make([]string, 0)
	if cscr.CType == SKETCH {
		if cscr.IsEmpty() {
			return result
		}
		for i := range cscr.CSCs {
			if i == 0 {
				result = cscr.CSCs[i].Get(item)
			} else {
				part_res := cscr.CSCs[i].Get(item)
				result = intersect(result, part_res)
			}
		}
		return result
	} else {
		if _, ok := cscr.HashMap[item]; !ok {
			return result
		}
		return cscr.HashMap[item]
	}
}

func (cscr *CSCR) GetWithCache(item string, cacheList *CSCCacheList) []string {
	result := make([]string, 0)
	if cscr.CType == SKETCH {
		if cscr.IsEmpty() {
			return result
		}
		for i := range cscr.CSCs {
			if i == 0 {
				result = cscr.CSCs[i].GetWithCache(item, cacheList.CSCCacheList[i])
			} else {
				part_res := cscr.CSCs[i].GetWithCache(item, cacheList.CSCCacheList[i])
				result = intersect(result, part_res)
			}
		}
		return result
	} else {
		if _, ok := cscr.HashMap[item]; !ok {
			return result
		}
		return cscr.HashMap[item]
	}
}

func NewCSCRWithEstimationWithCache(elementNum int, fingerprintSize int, slotNum int, maxKickAttempts int, partitionNum int, r int, cacheList *CSCCacheList) *CSCR {
	cscs := make([]*CSC, r)
	for i := range cscs {
		cscs[i] = NewCSCWithEstimationWithCache(elementNum, fingerprintSize, slotNum, maxKickAttempts, partitionNum, cacheList.CSCCacheList[i])
	}
	return &CSCR{
		CType:   SKETCH,
		CSCs:    cscs,
		HashMap: nil,
		R:       r,
	}
}

func intersect(a []string, b []string) []string {
	set := make(map[string]struct{})                 // 使用空结构体减少内存占用
	result := make([]string, 0, min(len(a), len(b))) // 预先分配内存，长度为 a 和 b 中较小的一个

	for _, val := range a {
		set[val] = struct{}{}
	}

	for _, val := range b {
		if _, exists := set[val]; exists {
			result = append(result, val)
			delete(set, val) // 如果元素已经找到，删除它以避免重复查找
		}
	}
	return result
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (cscr *CSCR) GetCSC(index int) *CSC {
	return cscr.CSCs[index]
}

func (cscr *CSCR) GetBitSize() int {
	if cscr.R == 0 || cscr.CType == HASHMAP {
		return 0
	}
	total_bit_size := 0
	for i := range cscr.CSCs {
		total_bit_size += cscr.CSCs[i].GetBitSize()
	}
	return total_bit_size
}

// 计算利用率
func (cscr *CSCR) GetUtilizationRate() float64 {
	if cscr.R == 0 || cscr.CType == HASHMAP {
		return 0
	}
	total_utilization := 0.0
	denominator := 0
	for i := range cscr.CSCs {
		if cscr.CSCs[i].GetUtilizationRate() != 0 {
			denominator++
			total_utilization += cscr.CSCs[i].GetUtilizationRate()
		}
	}
	return math.Round(total_utilization/float64(denominator)*100) / 100
}

func (cscr *CSCR) GetUtilizationCount() int {
	if cscr.R == 0 || cscr.CType == HASHMAP {
		return 0
	}
	total_utilization := 0
	for i := range cscr.CSCs {
		total_utilization += cscr.CSCs[i].Utilization_count
	}
	return total_utilization / cscr.R
}
