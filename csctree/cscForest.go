package csctree

import (
	"math"
	"sync"

	"github.com/liuys-dase/csc-tree/context"
)

type CSCForest struct {
	CSCForest []*CSCTree // CSCTree 数组
	Current   int        // 当前写入的 CSCTree 的索引
	Context   *context.Context
}

func NewCSCForest(context *context.Context) *CSCForest {
	return &CSCForest{
		// 创建一个 CSCTree 实例
		CSCForest: []*CSCTree{NewCSCTree(context)},
		Current:   0,
		Context:   context,
	}
}

func (cscForest *CSCForest) AddwithBlock(blockNumber int, txnStrings []string) {
	// 获取当前 CSCTree
	currentCSCTree := cscForest.CSCForest[cscForest.Current]
	// MODIFY
	if !cscForest.Context.Config.CSCTreeConfig.UseFlatten {
		currentCSCTree.AddWithBlock(blockNumber, txnStrings, cscForest.Context)
	} else {
		currentCSCTree.AddWithBlockWithKLeafs(blockNumber, txnStrings, cscForest.Context)
	}
	// currentCSCTree.AddWithBlock(blockNumber, txnStrings, cscForest.Context)
	// currentCSCTree.AddWithBlockWithKLeafs(blockNumber, txnStrings, cscForest.Context)
	// 每次添加一个 LeafNode 后，判断当前 CSCTree 是否已满
	if currentCSCTree.Full() {
		// 如果当前 CSCTree 已满，则创建一个新的 CSCTree
		cscForest.CSCForest = append(cscForest.CSCForest, NewCSCTree(cscForest.Context))
		cscForest.Current++
	}
}

func (cscForest *CSCForest) Get(item string) ([]Node, int64, int) {
	nodes := make([]Node, 0)
	cscrTime := int64(0)
	cscrCount := 0
	for _, t := range cscForest.CSCForest {
		if !t.IsEmpty() {
			// MODIFY
			if !cscForest.Context.Config.CSCTreeConfig.UseFlatten {
				nodes = append(nodes, t.Get(item)...)
				cscrTime += t.TimeCounter.GetCSCRTime()
				cscrCount++
				t.TimeCounter.Clear()
			} else {
				nodes = append(nodes, t.GetWithKLeafs(item)...)
				cscrTime += t.TimeCounter.GetCSCRTime()
				cscrCount++
				t.TimeCounter.Clear()
			}
		}
	}
	return nodes, cscrTime, cscrCount
}

func (cscForest *CSCForest) GetWithRange(item string, start_block int, end_block int) ([]Node, int64, int) {
	nodes := make([]Node, 0)
	cscrTime := int64(0)
	cscrCount := 0
	for _, t := range cscForest.CSCForest {
		if !t.IsEmpty() {
			// MODIFY
			if !cscForest.Context.Config.CSCTreeConfig.UseFlatten {
				nodes = append(nodes, t.GetWithRange(item, start_block, end_block)...)
				cscrTime += t.TimeCounter.GetCSCRTime()
				cscrCount++
				t.TimeCounter.Clear()
			} else {
				nodes = append(nodes, t.GetWithKLeafs(item)...)
				cscrTime += t.TimeCounter.GetCSCRTime()
				cscrCount++
				t.TimeCounter.Clear()
			}
		}
	}
	return nodes, cscrTime, cscrCount
}

// Get 方法使用多线程执行
func (cscForest *CSCForest) GetMultiThread(item string) []Node {
	var wg sync.WaitGroup
	nodeChannel := make(chan []Node, len(cscForest.CSCForest))

	for _, t := range cscForest.CSCForest {
		if !t.IsEmpty() {
			wg.Add(1)
			go func(tree *CSCTree) {
				defer wg.Done()
				nodeChannel <- tree.Get(item)
			}(t)
		}
	}

	// 等待所有 Goroutine 完成
	go func() {
		wg.Wait()
		close(nodeChannel)
	}()

	// 收集结果
	nodes := make([]Node, 0)
	for n := range nodeChannel {
		nodes = append(nodes, n...)
	}

	return nodes
}

func (cscForest *CSCForest) GetBitSize() int {
	total_bit_size := 0
	for _, t := range cscForest.CSCForest {
		if !t.IsEmpty() {
			total_bit_size += t.GetBitSize()
		}
	}
	return total_bit_size
}

func (cscForest *CSCForest) GetUtilizationRate() float64 {
	total_utilization := 0.0
	denominator := 0
	for _, t := range cscForest.CSCForest {
		if !t.IsEmpty() {
			total_utilization += t.GetUtilizationRate()
			denominator++
		}
	}
	return math.Round(total_utilization/float64(denominator)*100) / 100
}

// 获取指定的 CSCTree
func (cscForest *CSCForest) GetTreeByIndex(index int) *CSCTree {
	if index >= len(cscForest.CSCForest) {
		return nil
	}
	return cscForest.CSCForest[index]
}
