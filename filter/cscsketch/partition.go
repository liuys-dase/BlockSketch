package cscsketch

import "github.com/cespare/xxhash/v2"

type GlobalPartition struct {
	// 用于存储所有的分区
	Partitions []Partition
	// 哈希种子
	Seed uint64
}

type Partition struct {
	Blocks map[string]bool
}

func NewPartition() *Partition {
	return &Partition{
		Blocks: make(map[string]bool),
	}
}

// NewGlobalPartition 创建一个新的全局分区
func NewGlobalPartition(partitionNum int, seed uint64) *GlobalPartition {
	partitions := make([]Partition, partitionNum)
	for par := range partitions {
		partitions[par].Blocks = make(map[string]bool)
	}
	return &GlobalPartition{
		Partitions: partitions,
		Seed:       seed,
	}
}

func (p *GlobalPartition) Clear() {
	for i := range p.Partitions {
		p.Partitions[i].Blocks = make(map[string]bool)
	}
}

func (par *GlobalPartition) Add(key string) {
	parId := par.GetPartitionId(key)
	par.Partitions[parId].Blocks[key] = true
}

func (par *GlobalPartition) Get(index int) []string {
	keys := make([]string, 0)
	for key := range par.Partitions[index].Blocks {
		keys = append(keys, key)
	}
	return keys
}

func (par *GlobalPartition) GetPartitionId(key string) int {
	// 将 key 转换为 []byte 类型
	h := xxhash.NewWithSeed(par.Seed)
	h.Write([]byte(key))
	return int(h.Sum64() % uint64(len(par.Partitions)))
}

// 输出每一个 partition 中的元素数量
func (par *GlobalPartition) PrintPartition() {
	for i, partition := range par.Partitions {
		println("partition", i, ":", len(partition.Blocks))
	}
}
