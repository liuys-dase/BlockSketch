package compactfilter

import (
	"math"

	"github.com/liuys-dase/csc-tree/csctree"
	"github.com/liuys-dase/csc-tree/filter/basicfilter"
	"github.com/liuys-dase/csc-tree/filter/cscsketch"
)

type RootFilter struct {
	ColdFilter  *cscsketch.CSCR
	HotFilter   *basicfilter.BloomFilter
	PrefixFpNum int
	SuffixFpNum int
}

func NewRootFilterWithEstimation(cf_element_num int, cf_fingerprint_size int, cf_slot_num int,
	cf_max_attempts_num int, cf_partition_num int, cf_repeat_num int,
	bf_element_num int, bf_fpr float64, bf_hash_num int, suffix_fingerprint_size int) *RootFilter {
	coldFilter := cscsketch.NewCSCRWithEstimation(cf_element_num, cf_fingerprint_size, cf_slot_num,
		cf_max_attempts_num, cf_partition_num, cf_repeat_num)
	hotFilter := basicfilter.NewBloomFilter(bf_element_num, bf_fpr, bf_hash_num)
	return &RootFilter{
		ColdFilter:  coldFilter,
		HotFilter:   hotFilter,
		PrefixFpNum: cf_fingerprint_size,
		SuffixFpNum: suffix_fingerprint_size,
	}
}

// 只添加到 cold filter
func (rf *RootFilter) SingleAdd(item string, fileId string) bool {
	return rf.ColdFilter.Add(item, fileId)
}

// 添加到 cold filter 和 hot filter
func (rf *RootFilter) MultiAdd(item string, fileId string) bool {
	ret := rf.ColdFilter.Add(item, fileId)
	rf.HotFilter.Add(string(rf.GetSuffixFp(item)))
	return ret
}

// 获取指纹的后 SuffixFpNum 位
func (rf *RootFilter) GetSuffixFp(item string) []byte {
	// 获取指纹的 rf.PrefixFpNum+rf.SuffixFpNum 位
	fp := rf.ColdFilter.CSCs[0].FingerprintWithLength(item, rf.PrefixFpNum+rf.SuffixFpNum)
	return rf.Uint64ToBytes(fp, rf.SuffixFpNum)
}

// 将 value 的低 bitLength 位转换为字节数组
func (rf *RootFilter) Uint64ToBytes(value uint64, bitLength int) []byte {
	// 将高 64 - bitLength 位清零
	mask := uint64((1 << bitLength) - 1)
	value = value & mask
	// 计算需要的字节数
	byteLength := int(math.Ceil(float64(bitLength) / float64(8)))
	bytes := make([]byte, byteLength)
	// 从低位到高位依次取出
	for i := 0; i < byteLength; i++ {
		bytes[byteLength-1-i] = byte(value >> (i * 8))
	}
	return bytes
}

func (rf *RootFilter) Get(item string) []string {
	ret := make([]string, 0)
	fileList := rf.ColdFilter.Get(item)
	// 如果 cold filter 中没有，直接返回
	if len(fileList) == 0 {
		return ret
	} else {
		// 否则，需要进一步判断需要返回 cold part 还是 hot part
		// 继续在 hot filter 中查找
		hot := rf.HotFilter.Get(string(rf.GetSuffixFp(item)))
		cold_part, hot_part := SplitNodeIdList(fileList)
		if hot {
			ret = append(ret, hot_part...)
		} else {
			ret = append(ret, cold_part...)
		}
	}
	return ret
}

func SplitNodeIdList(nodeIdList []string) ([]string, []string) {
	cold_part := make([]string, 0)
	hot_part := make([]string, 0)
	for _, nodeId := range nodeIdList {
		if csctree.IsLeafNode(nodeId) {
			cold_part = append(cold_part, nodeId)
		} else {
			hot_part = append(hot_part, nodeId)
		}
	}
	return cold_part, hot_part
}
