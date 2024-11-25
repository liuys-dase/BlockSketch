package csctree

/*

	构造：自底向上，每两个节点构造一个父节点，直到根节点
	查询：自顶向下，根据查询条件，逐层过滤，直到叶子节点

*/

import (
	"math"
	"strconv"
	"time"

	"github.com/liuys-dase/csc-tree/block"
	"github.com/liuys-dase/csc-tree/context"
	"github.com/liuys-dase/csc-tree/filter/basicfilter"
	"github.com/liuys-dase/csc-tree/filter/cscsketch"
	"github.com/liuys-dase/csc-tree/timecounter"
)

type CSCTree struct {
	Root         Node
	queue        *Deque
	MaxLevel     int
	GlobalNid    int
	UseNodeIndex bool
	NodeIndex    map[int]Node
	HashGroup    *basicfilter.BFHashGroup
	CscCacheList *cscsketch.CSCCacheList
	TimeCounter  *timecounter.BlockSketchTimeCounter
}

func NewCSCTree(context *context.Context) *CSCTree {
	// node index
	useNodeIndex := context.Config.CSCTreeConfig.UseNodeIndex
	var nodeIndex map[int]Node
	if useNodeIndex {
		nodeIndex = make(map[int]Node)
	}
	// hash func
	hashGroup := basicfilter.NewBFHashGroup(context.Config.CSCTreeConfig.BfHashFuncNum)
	return &CSCTree{
		queue:        NewDeque(),
		MaxLevel:     context.Config.CSCTreeConfig.MaxLevel,
		GlobalNid:    1,
		UseNodeIndex: useNodeIndex,
		NodeIndex:    nodeIndex,
		HashGroup:    hashGroup,
		CscCacheList: cscsketch.NewCSCCacheList(context.Config.CSCTreeConfig.RepetitionNum),
		TimeCounter:  timecounter.NewBlockSketchTimeCounter(),
	}
}

// 查找一个 item 所在的全部叶子节点
func (t *CSCTree) Get(item string) []Node {
	// 清除缓存
	t.CscCacheList.Clear()
	res := make([]Node, 0)
	if t.IsEmpty() {
		return res
	}
	queue := NewDeque()
	queue.PushBack(NewQueryPlan(t.Root, nil, false, false))
	for queue.Size() > 0 {
		qp := queue.RemoveFromFront().(QueryPlan)
		node := qp.N
		switch n := node.(type) {
		// 如果是 RootNode，则将左孩子加入队列（只需要加入一个节点，另一个可以通过 sibling 指针获取）
		case *RootNode:
			queue.PushBack(NewQueryPlan(n.LeftChild, nil, false, false))
		case *InternalNode:
			// 如果是由 cscr 推入的中间节点，则无需检查 BloomFilter，直接将左孩子加入队列
			start_time := time.Now()
			hit := n.BloomFilter.GetWithHashGroup(item, t.HashGroup)
			t.TimeCounter.AddBFTime(start_time)
			if hit && !qp.IgnoreBfCheck {
				// 先检查是否在 BloomFilter 中，如果在，则将左右孩子加入队列
				// 将左孩子和兄弟节点的左孩子加入队列
				queue.PushBack(NewQueryPlan(n.LeftChild, n, true, false))
				siblingLeftChild := n.GetSiblingNode().GetLeftChild()
				queue.PushBack(NewQueryPlan(siblingLeftChild, n, true, false))
			} else {
				// 如果不在 BloomFilter 中，则需要进一步检查 CSCR
				// cscr_res := n.CSCR.Get(item)
				start_time := time.Now()
				cscr_res := n.CSCR.GetWithCache(item, t.CscCacheList)
				t.TimeCounter.AddCSCRTime(start_time)
				if len(cscr_res) == 0 && qp.IsPushedByBf {
					// 如果是由 BloomFilter 推入的节点，且 CSCR 为空，说明布隆过滤器假阳了，需要回溯检查上一层的 csc
					queue.PushBack(NewQueryPlan(qp.ParentNode, nil, false, true))
					continue
				}
				for _, nodeId := range cscr_res {
					nid, _ := strconv.Atoi(nodeId)
					// 向下遍历树，找到符合条件的节点
					foundNode := t.findNodeById(n, nid)
					if foundNode != nil {
						// 叶子节点直接返回
						if foundNode.GetNodeType() == LEAF {
							res = append(res, foundNode)
						} else {
							// 将节点加入队列（不会有 node == nil 的情况）
							queue.PushBack(NewQueryPlan(foundNode.GetLeftChild(), node, false, false))
							// cscr 如果返回的结果是中间节点，可以判断，如果是叶子节点，则将左孩子加入队列；
							// 如果是中间节点，则将左右孩子的左孩子分别加入队列
							// 虽然这种方式可以跳过一些 bf 的检查，但是可能会增加要检查的点的数量，从而导致最后的查询时间增加
							// if foundNode.GetLeftChild().GetNodeType() == LEAF {
							// 	queue.PushBack(NewQueryPlan(foundNode.GetLeftChild(), foundNode, false, false))
							// } else {
							// 	queue.PushBack(NewQueryPlan(foundNode.GetLeftChild().GetLeftChild(), foundNode, false, false))
							// 	queue.PushBack(NewQueryPlan(foundNode.GetRightChild().GetLeftChild(), foundNode, false, false))
							// }
						}
					}
				}
			}
		case *LeafNode:
			// 先检查是否在 BloomFilter 中，如果在，则将其与兄弟节点加入 res
			start_time := time.Now()
			hit := n.BloomFilter.GetWithHashGroup(item, t.HashGroup)
			t.TimeCounter.AddBFTime(start_time)
			if hit {
				res = append(res, n, n.GetSiblingNode())
			} else {
				// 如果不在 BloomFilter 中，则需要进一步检查 CSCR
				// cscr_res := n.CSCR.Get(item)
				start_time := time.Now()
				cscr_res := n.CSCR.GetWithCache(item, t.CscCacheList)
				t.TimeCounter.AddCSCRTime(start_time)
				if len(cscr_res) == 0 && qp.IsPushedByBf {
					// 如果是由 BloomFilter 推入的节点，且 CSCR 为空，则说明父节点的 bf 假阳了
					queue.PushBack(NewQueryPlan(qp.ParentNode, nil, false, true))
					continue
				}
				for _, nodeId := range cscr_res {
					// 由于已经是叶子节点，所以不需要向下遍历
					nid, _ := strconv.Atoi(nodeId)
					if n.GetNid() == nid {
						res = append(res, n)
					} else {
						res = append(res, n.GetSiblingNode())
					}
				}
			}
		}
	}
	return res
}

// 添加叶子节点
func (t *CSCTree) Add(l *LeafNode, ctx *context.Context) bool {
	// 判断当前 Level 是否已经是最大 Level
	if t.Root != nil && t.Level() == t.MaxLevel {
		return false
	}

	// 如果树为空，直接将叶子节点作为根节点
	if t.Root == nil {
		t.Root = l
		t.queue.PushBack(l)
		return true
	}

	// 根据队尾节点类型进行处理
	switch t.queue.Back().(type) {

	// 如果已经有 RootNode，表示树已经构建完成，不再添加节点
	case *RootNode:
		return false
	// 如果是 InternalNode，则直接追加到队尾
	case *InternalNode:
		t.queue.PushBack(l)
		return true
		// 如果是 LeafNode，则与 l 需要生成一个新的 InternalNode，新的 InternalNode 可能继续向上构建
	case *LeafNode:
		// 从队尾取出一个节点，并且生成一个 InternalNode
		newNode := t.CreateInternalNode(t.queue.RemoveFromBack().(*LeafNode), l, ctx)
		for {
			// 如果新生成的节点已经是最大 Level，先转换为 RootNode 类型，然后设置为 Root
			if newNode.Level == t.MaxLevel {
				t.Root = t.CreateRootNode(newNode, ctx)
				return true
			}
			// 尝试获取队尾的元素并转换为 InternalNode
			lastNode, ok := t.queue.Back().(*InternalNode)
			if !ok {
				// 如果队尾已经没有元素（比如说仅有两个区块时），则直接将新的 InternalNode 设置为 RootNode 并加入队尾
				t.Root = newNode
				t.queue.PushBack(newNode)
				return true
			}
			if lastNode.Level != newNode.Level {
				// 如果不同，则直接将新的 InternalNode 加入队尾
				t.queue.PushBack(newNode)
				return true
			}
			// 如果相同，则需要继续向上构建
			newNode = t.CreateInternalNode(t.queue.RemoveFromBack().(*InternalNode), newNode, ctx)
		}
	}
	return false
}

// 创建一个 叶子节点，然后添加到 CSCTree 中
func (t *CSCTree) AddWithBlock(blockNumber int, txns []string, ctx *context.Context) bool {
	leafNode := NewLeafNode(blockNumber)
	leafNode.SetNid(t.GlobalNid)
	leafNode.SetSenderSet(block.NewAccountSetFromBlock(blockNumber, txns, true, leafNode.Nid))
	t.updateIndex(leafNode)
	t.GlobalNid++
	return t.Add(leafNode, ctx)
}

// csctree 创建 cscr 的函数
func (t *CSCTree) InitializeCSCR(node Node, intersection []string, ctx *context.Context) *cscsketch.CSCR {
	if len(intersection) == 0 {
		return cscsketch.NewEmptyCSCR()
	}
	senderSet := node.GetSenderSet().BatchGetWithDelete(intersection)
	// 这里根据 node 的 level 判断是否直接用 hashmap 还是 cscr
	if node.GetLevel()-1 < ctx.Config.CSCTreeConfig.SketchLevel {
		cscr := cscsketch.NewCSCRWithHashMap()
		for v, nodeId := range senderSet {
			cscr.Add(v, strconv.Itoa(nodeId))
			// cscr.Add(v, nodeId)
		}
		return cscr
	} else {
		cscr := t.NewCSCRWithEstimation(len(senderSet), ctx, node.GetRange().Size())
		cscr.BatchAdd(senderSet)
		return cscr
	}
}

// 由 CSCTree 调用，生成新的 InternalNode（InternalNode.NewInternalNode仅仅负责生成新的节点，其他的处理逻辑放在这边，避免 Node 内部需要存储 context）
func (t *CSCTree) CreateInternalNode(leftNode Node, rightNode Node, ctx *context.Context) *InternalNode {
	// log.Printf("CreateInternalNode: %v, %v\n", leftNode.GetNid(), rightNode.GetNid())
	// 生成新的 InternalNode
	internalNode := NewInternalNode(leftNode, rightNode)
	internalNode.SetNid(t.GlobalNid)
	t.updateIndex(internalNode)
	t.GlobalNid++

	// step1. 计算 leftNode 和 rightNode 的 TransactionMap 的交集
	intersection := leftNode.GetSenderSet().Intersect(rightNode.GetSenderSet())

	// log.Printf("intersection: %v\n", intersection)

	// step2. 将交集加入 children 的 bf
	bf := t.NewBloomFilter(len(intersection), ctx)
	bf.BatchAdd(intersection)
	leftNode.SetBloomFilter(bf)
	rightNode.SetBloomFilter(bf)

	// step3. 判断 leftNode 和 rightNode 的类型，如果是 InternalNode，则需要将交集加入他们的children 的 cscr；如果是 LeafNode，则不需要
	if leftNode.GetNodeType() == INTERNAL {
		// 将交集加入 CSCR，同时将各自的 senderSet 中的交集删除
		cscr_left := t.InitializeCSCR(leftNode, intersection, ctx)
		// log.Printf("leftNode: %v, level: %v, elementNum: %v\n", leftNode.GetNid(), leftNode.GetLevel(), len(intersection))
		cscr_right := t.InitializeCSCR(rightNode, intersection, ctx)
		// 将 CSCR 加入 children
		leftNode.GetLeftChild().SetCSCR(cscr_left)
		leftNode.GetRightChild().SetCSCR(cscr_left)
		rightNode.GetLeftChild().SetCSCR(cscr_right)
		rightNode.GetRightChild().SetCSCR(cscr_right)
	}

	// step4. 将 children 剩余的 set（例如 a[1,1]、b[3,3]） 和 intersection（例如 (a,[1,4])）合并后加入 InternalNode 的 SenderSet
	newSenderSet := leftNode.GetSenderSet().Union(rightNode.GetSenderSet())
	for _, v := range intersection {
		newSenderSet.Add(v, internalNode.GetNid())
	}
	internalNode.SetSenderSet(newSenderSet)
	// 删除 children 的 set
	leftNode.SetSenderSet(nil)
	rightNode.SetSenderSet(nil)
	return internalNode
}

func (t *CSCTree) CreateRootNode(internalNode *InternalNode, ctx *context.Context) *RootNode {
	// 如果节点被转换为 RootNode，则需要将当前节点暂存的 SenderSet 中的元素存到两个孩子节点的 CSCR 中
	senderSet := internalNode.GetSenderSet()
	// 需要删除和 internalNode 具有相同 BlockRange 的元素
	for v, nodeId := range senderSet.GetAccount() {
		if nodeId == internalNode.GetNid() {
			senderSet.Delete(v)
		}
	}
	// 构造 cscr
	cscr := t.NewCSCRWithEstimation(len(senderSet.GetAccount()), ctx, internalNode.GetRange().Size())

	// for v, nodeId := range senderSet.GetAccount() {
	// 	fmt.Printf("%v,%v\n", v, nodeId)
	// }

	// 添加元素
	cscr.BatchAdd(senderSet.GetAccount())

	// 将 CSCR 加入两个孩子节点
	internalNode.GetLeftChild().SetCSCR(cscr)
	internalNode.GetRightChild().SetCSCR(cscr)
	// 清空 SenderSet
	internalNode.SetSenderSet(nil)
	rootNode := internalNode.ToRootNode()
	t.updateIndex(rootNode)
	return rootNode
}

func (t *CSCTree) NewBloomFilter(elementNum int, ctx *context.Context) *basicfilter.BloomFilter {
	return basicfilter.NewBloomFilterWithHashGroup(elementNum,
		ctx.Config.CSCTreeConfig.BfFalsePositiveRate,
		ctx.Config.CSCTreeConfig.BfHashFuncNum,
		t.HashGroup)
}

// 需要修改
func (t *CSCTree) NewCSCRWithEstimation(elementNum int, ctx *context.Context, blockRange int) *cscsketch.CSCR {
	fingerprintSize := ctx.Config.CSCTreeConfig.FingerprintSize
	fingerprintNum := ctx.Config.CSCTreeConfig.FingerprintNum
	maxKickAttempts := ctx.Config.CSCTreeConfig.MaxKickAttempts
	minPartitionNum := ctx.Config.CSCTreeConfig.PartitionNum
	// 选择一个合适的 partitionNum
	partitionNum := blockRange/ctx.Config.CSCTreeConfig.MaxElementNumPerPar + 1
	if partitionNum < minPartitionNum {
		partitionNum = minPartitionNum
	}
	repetitionNum := ctx.Config.CSCTreeConfig.RepetitionNum
	// return cscsketch.NewCSCRWithEstimation(elementNum, fingerprintSize, fingerprintNum, maxKickAttempts, partitionNum, repetitionNum)
	return cscsketch.NewCSCRWithEstimationWithCache(elementNum, fingerprintSize, fingerprintNum, maxKickAttempts, partitionNum, repetitionNum, t.CscCacheList)
}

// 从 RootNode 开始 BFS 遍历，返回 []Node
func (t *CSCTree) BFS() []Node {
	var nodes []Node
	if t.Root == nil {
		return nodes
	}
	queue := NewDeque()
	queue.PushBack(t.Root)
	for queue.Size() > 0 {
		node := queue.RemoveFromFront().(Node)
		nodes = append(nodes, node)
		switch n := node.(type) {
		case *RootNode:
			queue.PushBack(n.LeftChild)
			queue.PushBack(n.RightChild)
		case *InternalNode:
			queue.PushBack(n.LeftChild)
			queue.PushBack(n.RightChild)
		}
	}
	return nodes
}

// 计算 csctree 中所有 csc 和 bf 的 bit size
func (t *CSCTree) GetBitSize() int {
	nodes := t.BFS()
	total_bit_size := 0
	for _, node := range nodes {
		switch n := node.(type) {
		case *InternalNode:
			// log.Printf("	InternalNode: %v, bit size of cscr: %v, ur: %v, count: %v", n.GetRange(), n.CSCR.GetBitSize(), n.CSCR.GetUtilizationRate(), n.CSCR.GetUtilizationCount())
			total_bit_size += n.BloomFilter.GetBitSize()
			total_bit_size += n.CSCR.GetBitSize()
		case *LeafNode:
			// log.Printf("	InternalNode: %v, bit size of cscr: %v, ur: %v, count: %v", n.GetRange(), n.CSCR.GetBitSize(), n.CSCR.GetUtilizationRate(), n.CSCR.GetUtilizationCount())
			total_bit_size += n.BloomFilter.GetBitSize()
			total_bit_size += n.CSCR.GetBitSize()
		}
	}
	return total_bit_size / 2
}

// 计算 csctree 中所有 cscr 的利用率
func (t *CSCTree) GetUtilizationRate() float64 {
	nodes := t.BFS()
	total_utilization_rate := 0.0
	denominator := 0
	for _, node := range nodes {
		switch n := node.(type) {
		case *InternalNode:
			if n.CSCR.GetUtilizationRate() != 0 {
				total_utilization_rate += n.CSCR.GetUtilizationRate()
				denominator++
			}
		case *LeafNode:
			if n.CSCR.GetUtilizationRate() != 0 {
				total_utilization_rate += n.CSCR.GetUtilizationRate()
				denominator++
			}
		}
	}
	// 分子分母都是真实值的 2 倍，正好抵消
	return math.Round(total_utilization_rate/float64(denominator)*100) / 100
}

type QueryPlan struct {
	N             Node
	ParentNode    Node
	IsPushedByBf  bool
	IgnoreBfCheck bool
}

func NewQueryPlan(n Node, parentNode Node, isPushedByBf bool, ignoreBfCheck bool) QueryPlan {
	return QueryPlan{
		N:             n,
		ParentNode:    parentNode,
		IsPushedByBf:  isPushedByBf,
		IgnoreBfCheck: ignoreBfCheck,
	}
}

func (t *CSCTree) IsEmpty() bool {
	return t.Root == nil
}

func (t *CSCTree) Level() int {
	return t.Root.GetLevel()
}

func (t *CSCTree) Full() bool {
	if t.Root == nil {
		return false
	}
	return t.Root.GetLevel() == t.MaxLevel
}

// 向下遍历树，找到具有相同 BlockRange 的节点
func (t *CSCTree) findNodeById(leftNode Node, nodeId int) Node {

	if leftNode.GetNid() < nodeId && leftNode.GetSiblingNode().GetNid() < nodeId {
		return nil
	}
	if t.UseNodeIndex {
		if node, ok := t.NodeIndex[nodeId]; ok {
			return node
		} else {
			return nil
		}
	} else {
		queue := NewDeque()
		// 判断 nodeId 是在左子树还是右子树
		if nodeId <= leftNode.GetNid() {
			queue.PushBack(leftNode)
		} else {
			queue.PushBack(leftNode.GetSiblingNode())
		}
		for queue.Size() > 0 {
			node := queue.RemoveFromFront().(Node)
			switch n := node.(type) {
			case *InternalNode:
				if n.GetNid() == nodeId {
					return node
				}
				queue.PushBack(n.LeftChild)
				queue.PushBack(n.RightChild)
			case *LeafNode:
				if n.GetNid() == nodeId {
					return node
				}
			}
		}
		return nil
	}
}

func (t *CSCTree) updateIndex(node Node) {
	if t.UseNodeIndex {
		t.NodeIndex[node.GetNid()] = node
	}
}

// 查找一个 item 所在的全部叶子节点
func (t *CSCTree) GetWithRange(item string, start_block int, end_block int) []Node {
	block_range := block.NewBlockRange(start_block, end_block)
	// 清除缓存
	t.CscCacheList.Clear()
	res := make([]Node, 0)
	if t.IsEmpty() {
		return res
	}
	queue := NewDeque()
	queue.PushBack(NewQueryPlan(t.Root, nil, false, false))
	for queue.Size() > 0 {
		qp := queue.RemoveFromFront().(QueryPlan)
		node := qp.N
		switch n := node.(type) {
		// 如果是 RootNode，则将左孩子加入队列（只需要加入一个节点，另一个可以通过 sibling 指针获取）
		case *RootNode:
			if n.GetRange().Intersect(block_range) {
				queue.PushBack(NewQueryPlan(n.LeftChild, nil, false, false))
			}
		case *InternalNode:
			// 如果当前结点的 range 和 block_range 没有重合，则没必要检查
			if !n.GetRange().Intersect(block_range) && !n.GetSiblingNode().GetRange().Intersect(block_range) {
				break
			}
			// 如果是由 cscr 推入的中间节点，则无需检查 BloomFilter，直接将左孩子加入队列
			start_time := time.Now()
			hit := n.BloomFilter.GetWithHashGroup(item, t.HashGroup)
			t.TimeCounter.AddBFTime(start_time)
			if hit && !qp.IgnoreBfCheck {
				// 先检查是否在 BloomFilter 中，如果在，则将左右孩子加入队列
				// 将左孩子和兄弟节点的左孩子加入队列
				if n.LeftChild.GetRange().Intersect(block_range) || n.LeftChild.GetSiblingNode().GetRange().Intersect(block_range) {
					queue.PushBack(NewQueryPlan(n.LeftChild, n, true, false))
				}
				siblingLeftChild := n.GetSiblingNode().GetLeftChild()
				if siblingLeftChild.GetRange().Intersect(block_range) || siblingLeftChild.GetSiblingNode().GetRange().Intersect(block_range) {
					queue.PushBack(NewQueryPlan(siblingLeftChild, n, true, false))
				}
			} else {
				// 如果不在 BloomFilter 中，则需要进一步检查 CSCR
				// cscr_res := n.CSCR.Get(item)
				start_time := time.Now()
				cscr_res := n.CSCR.GetWithCache(item, t.CscCacheList)
				t.TimeCounter.AddCSCRTime(start_time)
				if len(cscr_res) == 0 && qp.IsPushedByBf {
					// 如果是由 BloomFilter 推入的节点，且 CSCR 为空，说明布隆过滤器假阳了，需要回溯检查上一层的 csc
					queue.PushBack(NewQueryPlan(qp.ParentNode, nil, false, true))
					continue
				}
				for _, nodeId := range cscr_res {
					nid, _ := strconv.Atoi(nodeId)
					// 向下遍历树，找到符合条件的节点
					foundNode := t.findNodeById(n, nid)
					if foundNode != nil {
						// 叶子节点直接返回
						if foundNode.GetNodeType() == LEAF {
							if foundNode.GetRange().Intersect(block_range) {
								res = append(res, foundNode)
							}
						} else {
							// 将节点加入队列（不会有 node == nil 的情况）
							if foundNode.GetLeftChild().GetRange().Intersect(block_range) || foundNode.GetLeftChild().GetSiblingNode().GetRange().Intersect(block_range) {
								queue.PushBack(NewQueryPlan(foundNode.GetLeftChild(), node, false, false))
							}
						}
					}
				}
			}
		case *LeafNode:
			// 如果当前结点的 range 和 block_range 没有重合，则没必要检查
			if !n.GetRange().Intersect(block_range) && !n.GetSiblingNode().GetRange().Intersect(block_range) {
				break
			}
			// 先检查是否在 BloomFilter 中，如果在，则将其与兄弟节点加入 res
			start_time := time.Now()
			hit := n.BloomFilter.GetWithHashGroup(item, t.HashGroup)
			t.TimeCounter.AddBFTime(start_time)
			if hit {
				if n.GetRange().Intersect(block_range) {
					res = append(res, n)
				}
				if n.GetSiblingNode().GetRange().Intersect(block_range) {
					res = append(res, n.GetSiblingNode())
				}
			} else {
				// 如果不在 BloomFilter 中，则需要进一步检查 CSCR
				// cscr_res := n.CSCR.Get(item)
				start_time := time.Now()
				cscr_res := n.CSCR.GetWithCache(item, t.CscCacheList)
				t.TimeCounter.AddCSCRTime(start_time)
				if len(cscr_res) == 0 && qp.IsPushedByBf {
					// 如果是由 BloomFilter 推入的节点，且 CSCR 为空，则说明父节点的 bf 假阳了
					queue.PushBack(NewQueryPlan(qp.ParentNode, nil, false, true))
					continue
				}
				for _, nodeId := range cscr_res {
					// 由于已经是叶子节点，所以不需要向下遍历
					nid, _ := strconv.Atoi(nodeId)
					if n.GetNid() == nid {
						if n.GetRange().Intersect(block_range) {
							res = append(res, n)
						}
					} else {
						if n.GetSiblingNode().GetRange().Intersect(block_range) {
							res = append(res, n.GetSiblingNode())
						}
					}
				}
			}
		}
	}
	return res
}
