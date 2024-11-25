package csctree

import (
	"strconv"
	"time"

	"github.com/liuys-dase/csc-tree/block"
	"github.com/liuys-dase/csc-tree/context"
	"github.com/liuys-dase/csc-tree/filter/cscsketch"
)

func (t *CSCTree) AddWithKLeafs(l *LeafNode, ctx *context.Context) bool {
	if t.Root != nil && t.Level() == t.MaxLevel {
		return false
	}

	if t.Root == nil {
		t.Root = l
		t.queue.PushBack(l)
		return true
	}

	// 当队列中有元素时
	switch t.queue.Back().(type) {

	case *RootNode:
		return false
	case *InternalNode:
		t.queue.PushBack(l)
		return true
	case *FlattenNode:
		t.queue.PushBack(l)
		return true
	case *LeafNode:
		// 当前队尾是 LeafNode，检查队尾前 k 个节点是否都是 LeafNode
		leafNodeList := make([]*LeafNode, 0)
		// 首先将队尾的元素加入 leafNodeList
		leafNodeList = append(leafNodeList, t.queue.Back().(*LeafNode))
		for i := 2; i <= ctx.Config.CSCTreeConfig.LeafNum-1; i++ {
			if t.queue.KBack(i) == nil {
				// 如果队列长度不足 k，则直接将 l 加入队尾
				t.queue.PushBack(l)
				return true
			} else {
				// 如果该节点并不是 LeafNode，则直接将 l 加入队尾
				if _, ok := t.queue.KBack(i).(*LeafNode); !ok {
					t.queue.PushBack(l)
					return true
				} else {
					leafNodeList = append(leafNodeList, t.queue.KBack(i).(*LeafNode))
				}
			}
		}
		// 除非当前队尾前 k 个节点都是 LeafNode，否则不会执行到这里
		leafNodeList = append(leafNodeList, l)
		// 删除队列中的叶子结点
		for i := 1; i <= ctx.Config.CSCTreeConfig.LeafNum-1; i++ {
			t.queue.RemoveFromBack()
		}
		// 构造 flattenNode
		flattenNode := t.CreateFlattenNode(leafNodeList, ctx)
		// 如果此时队列为空，则直接将 flattenNode 加入队尾
		if t.queue.Size() == 0 {
			t.queue.PushBack(flattenNode)
			return true
		}
		// 如果队列不为空，判断是否需要继续向上构建（当前队尾是不是 Flatten）
		if _, ok := t.queue.Back().(*FlattenNode); ok {
			// 如果队尾是 Flatten，则两个 FlattenNode 合并成 internalNode
			newNode := t.CreateInternalNodeUponFlatten(t.queue.RemoveFromBack().(*FlattenNode), flattenNode, ctx)
			// 然后继续判断是否要递归向上构建
			for {
				if newNode.Level == t.MaxLevel {
					t.Root = t.CreateRootNode(newNode, ctx)
					return true
				}
				lastNode, ok := t.queue.Back().(*InternalNode)
				if !ok {
					t.Root = newNode
					t.queue.PushBack(newNode)
					return true
				}
				if lastNode.Level != newNode.Level {
					t.queue.PushBack(newNode)
					return true
				}
				// 除了 FlattenNode 之外，其他节点合并都采用 CreateInternalNode
				newNode = t.CreateInternalNode(t.queue.RemoveFromBack().(*InternalNode), newNode, ctx)
			}
		} else {
			// 如果队尾不是 Flatten，则直接将 FlattenNode 加入队尾
			t.queue.PushBack(flattenNode)
			return true
		}
	}
	return false
}

// step1. 将所有 LeafNode 的 senderSet 合并成一个 AccountMap
// step2. 遍历 AccountMap，将 len(value) > 1 的 key 提取出来存储 TmpAccountMap
func (t *CSCTree) CreateFlattenNode(leafNodeList []*LeafNode, ctx *context.Context) *FlattenNode {
	// step1
	flattenNode := NewFlattenNode(leafNodeList)
	flattenNode.SetNid(t.GlobalNid)
	t.updateIndex(flattenNode)
	t.GlobalNid++

	// step2
	// accountMap : <a,1> <a,4> <b,2>   tmpAccountMap : nil
	// accountMap : <a,5> <b,2>         tmpAccountMap : <a,1> <a,4>
	tmpAccountMap := NewAccountMap()
	mm := flattenNode.AccountMap.Map
	for addr, nidList := range mm {
		// len > 1 说明这个地址在多个叶子节点中出现过
		if len(nidList.NidList) > 1 {
			// 存入 tmpAccountMap
			tmpAccountMap.Map[addr] = nidList
			// 清空 accountMap[addr]
			mm[addr] = NewUniqueNidList()
			// 存入 <addr, Nid>
			mm[addr].Insert(flattenNode.Nid)
		}
	}
	flattenNode.TmpAccountMap = tmpAccountMap
	// log.Printf("range of flattenNode: %v\n", flattenNode.GetRange().ToString())
	// log.Printf("accountMap of flattenNode %v: %v\n", flattenNode.Nid, flattenNode.AccountMap.ToString())
	// log.Printf("tmpAccountMap of flattenNode %v: %v\n", flattenNode.Nid, flattenNode.TmpAccountMap.ToString())
	return flattenNode
}

func (t *CSCTree) CreateInternalNodeUponFlatten(leftNode *FlattenNode, rightNode *FlattenNode, ctx *context.Context) *InternalNode {
	// log.Printf("CreateInternalNodeUponFlatten: %v, %v\n", leftNode.GetNid(), rightNode.GetNid())
	internalNode := NewInternalNode(leftNode, rightNode)
	internalNode.SetNid(t.GlobalNid)
	t.updateIndex(internalNode)
	t.GlobalNid++

	// step1. 计算 leftNode 和 rightNode 的 AccountMap 的交集
	intersection := leftNode.AccountMap.Intersect(rightNode.AccountMap)
	// log.Printf("intersection of %v and %v: %v\n", leftNode.GetNid(), rightNode.GetNid(), intersection)

	// step2. 将交集加入 leftNode 和 rightNode 的 bf
	bf := t.NewBloomFilter(len(intersection), ctx)
	bf.BatchAdd(intersection)
	leftNode.SetBloomFilter(bf)
	rightNode.SetBloomFilter(bf)

	// step3. 生成 FlattenCSCR
	cscr_left := t.InitializeFlattenCSCR(leftNode, intersection, ctx)
	cscr_right := t.InitializeFlattenCSCR(rightNode, intersection, ctx)
	leftNode.FlattenCSCR = cscr_left
	rightNode.FlattenCSCR = cscr_right

	// step4. 生成新的 accountMap 向上传递
	// 此时，AccountMap 中只保留了没有交集的元素，交集元素已经被删除。因此，可以直接转换为 AccountSet
	newSenderSet := leftNode.AccountMap.ToAccountSet().Union(rightNode.AccountMap.ToAccountSet())
	for _, v := range intersection {
		newSenderSet.Add(v, internalNode.GetNid())
	}

	// log.Printf("newSenderSet of internalNode %v: %v\n", internalNode.GetNid(), newSenderSet.ToString())

	internalNode.SetSenderSet(newSenderSet)

	// 删除 leftNode 和 rightNode 的 AccountMap
	leftNode.AccountMap = nil
	leftNode.TmpAccountMap = nil
	rightNode.AccountMap = nil
	rightNode.TmpAccountMap = nil

	// log.Printf("New internal node level: %v\n", internalNode.Level)
	return internalNode
}

// 初始化 FlattenNode 的 FlattenCSCR
// step1. 根据 intersection 找到 FlattenNode 中的 AccountMap 中的数据
// step2. 如果是 <addr, Nid> 无需处理；如果是 <addr, 1> 等，则需要将其加入 FlattenCSCR
// accountMap : <a,5> <b,1> <c,2>
// accountMap : <b,1> <c,2>
func (t *CSCTree) InitializeFlattenCSCR(node *FlattenNode, intersection []string, ctx *context.Context) *cscsketch.CSCR {
	// 提取 FlattenNode 中 AccountMap 中的数据
	accountMap := node.AccountMap.BatchGetWithDelete(intersection)

	// log.Printf("(before intersection) accountMap of %v: %v\n", node.GetNid(), accountMap.ToString())

	for addr, nidList := range accountMap.Map {
		// 如果 nid 与 Nid 一致，则直接从 accountSet 中删除，因为这些点已经在 tmpAccountMap 中了
		for nid := range nidList.NidList {
			if nid == node.GetNid() {
				delete(accountMap.Map, addr)
				break
			}
		}
	}

	// log.Printf("(after intersection) accountMap of %v: %v\n", node.GetNid(), accountMap.ToString())
	am := node.TmpAccountMap.Union(accountMap)
	// 构造 FlattenCSCR，既需要考虑当前 accountMap 中的元素，还需要考虑 tmpAccountMap 中的元素
	flattenCSCR := t.NewCSCRWithEstimation(am.Size(), ctx, node.GetRange().Size())

	// log.Printf("am of %v: %v\n", node.GetNid(), am.ToString())

	// 如果 am 为空，则 flattenCSCR 也为空，直接返回
	if am.Size() == 0 {
		return flattenCSCR
	}

	retry_flag := true
	for retry_flag {
		for k, nidList := range am.Map {
			for nid := range nidList.NidList {
				if !flattenCSCR.Add(k, strconv.Itoa(nid)) {
					flattenCSCR.Double()
					retry_flag = true
					break
				}
				retry_flag = false
			}
			if retry_flag {
				break
			}
		}
	}
	// if node.GetRange().Start == 18000124 && node.GetRange().End == 18000127 {
	// 	target := "0xd108fd0e8c8e71552a167e7a44ff1d345d233ba6"
	// 	if _, ok := am.Map[target]; ok {
	// 		log.Printf("nid list: %v\n", am.Map[target].NidList)
	// 	} else {
	// 		log.Printf("not found\n")
	// 	}
	// 	log.Printf("========= checkpoint =======: %v\n", flattenCSCR.Get(target))
	// }
	return flattenCSCR
}

// 带有 FlattenNode 的查询
func (t *CSCTree) GetWithKLeafs(item string) []Node {
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
			// log.Printf("check internal node: %v\n", n.GetRange())
			start_time := time.Now()
			hit := n.BloomFilter.GetWithHashGroup(item, t.HashGroup)
			t.TimeCounter.AddBFTime(start_time)
			if hit && !qp.IgnoreBfCheck {
				// log.Printf("bloom filter (internal node) between %v and %v is true\n", n.GetRange(), n.GetSiblingNode().GetRange())
				// log.Printf("	put %v and %v into queue\n", n.GetLeftChild().GetRange(), n.GetSiblingNode().GetLeftChild().GetRange())
				// 先检查是否在 BloomFilter 中，如果在，则将左右孩子加入队列
				// 将左孩子和兄弟节点的左孩子加入队列
				queue.PushBack(NewQueryPlan(n.LeftChild, n, true, false))
				queue.PushBack(NewQueryPlan(n.GetSiblingNode().GetLeftChild(), n, true, false))
			} else {
				// log.Printf("bloom filter (internal node) between %v and %v is false\n", n.GetRange(), n.GetSiblingNode().GetRange())
				// 如果不在 BloomFilter 中，则需要进一步检查 CSCR
				start_time := time.Now()
				cscr_res := n.CSCR.GetWithCache(item, t.CscCacheList)
				// cscr_res := n.CSCR.Get(item)
				t.TimeCounter.AddCSCRTime(start_time)
				// log.Printf("	result of cscr: %v\n", cscr_res)
				if len(cscr_res) == 0 && qp.IsPushedByBf {
					// 如果是由 BloomFilter 推入的节点，且 CSCR 为空，说明布隆过滤器假阳了，需要回溯检查上一层的 csc
					queue.PushBack(NewQueryPlan(qp.ParentNode, nil, false, true))
					continue
				}
				for _, nodeId := range cscr_res {
					// 向下遍历树，找到符合条件的节点
					// log.Printf("	search node: %v\n", nodeId)
					nid, _ := strconv.Atoi(nodeId)
					foundNode := t.findNodeById(n, nid)
					if foundNode != nil {
						// log.Printf("	found node: %v\n", foundNode.GetRange())
						// 叶子节点直接返回
						if foundNode.GetNodeType() == LEAF {
							// log.Printf("    foundNode is leaf node\n")
							res = append(res, foundNode)
						} else if foundNode.GetNodeType() == FLATTEN {
							// log.Printf("    foundNode is flatten node\n")
							// 如果找到的节点是 FlattenNode，则直接将其 flattenCSCR 中的节点加入 res
							res = append(res, t.searchFlattenCSCR(foundNode.(*FlattenNode), item)...)
							// queue.PushBack(NewQueryPlan(foundNode, foundNode, false, false))
						} else {
							// log.Printf("    foundNode is internal node\n")
							queue.PushBack(NewQueryPlan(foundNode.GetLeftChild(), foundNode, false, false))
							// if foundNode.GetLeftChild().GetNodeType() == LEAF {
							// 	log.Printf("    child of foundNode is leaf node\n")
							// 	queue.PushBack(NewQueryPlan(foundNode.GetLeftChild(), foundNode, false, false))
							// } else if foundNode.GetLeftChild().GetNodeType() == FLATTEN {
							// 	// todo
							// 	queue.PushBack(NewQueryPlan(foundNode.GetLeftChild(), foundNode, false, false))
							// } else {
							// 	log.Printf("    child of foundNode is not leaf node\n")
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
				// log.Printf("bloom filter (leaf node) between %v and %v is true\n", n.GetRange(), n.GetSiblingNode().GetRange())
				// log.Printf("	put %v and %v into res\n", n.GetRange(), n.GetSiblingNode().GetRange())
				res = append(res, n, n.GetSiblingNode())
			} else {
				// 如果不在 BloomFilter 中，则需要进一步检查 CSCR
				start_time := time.Now()
				cscr_res := n.CSCR.GetWithCache(item, t.CscCacheList)
				// cscr_res := n.CSCR.Get(item)
				t.TimeCounter.AddCSCRTime(start_time)
				// log.Printf("bloom filter (leaf node) between %v and %v is false\n", n.GetRange(), n.GetSiblingNode().GetRange())
				// log.Printf("	result of cscr: %v\n", cscr_res)
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
		case *FlattenNode:
			// 先检查 bloomFilter，如果在，直接检查两个 FlattenNode 的 FlattenCSCR
			// log.Printf("check flattenNode: %v\n", n.GetRange())
			start_time := time.Now()
			hit := n.BloomFilter.GetWithHashGroup(item, t.HashGroup)
			t.TimeCounter.AddBFTime(start_time)
			if hit && !qp.IgnoreBfCheck {
				// log.Printf("bloom filter (flatten node) between %v and %v is true\n", n.GetRange(), n.GetSiblingNode().GetRange())
				// 分别查左右节点
				left_res := t.searchFlattenCSCR(n, item)
				right_res := t.searchFlattenCSCR(n.GetSiblingNode().(*FlattenNode), item)
				// 防止假阳
				if (len(left_res) == 0 || len(right_res) == 0) && qp.IsPushedByBf {
					// 把自己重新加入队列，并且标记为忽略 BloomFilter 检查
					// 因为如果两个 flattencscr 的结果都为空，说明是 flattennode 的 bloomfilter 假阳了
					queue.PushBack(NewQueryPlan(qp.N, nil, false, true))
					continue
				} else {
					res = append(res, left_res...)
					res = append(res, right_res...)
				}
				// // 先查左节点
				// res = append(res, t.searchFlattenCSCR(n, item)...)
				// // 再查右节点
				// res = append(res, t.searchFlattenCSCR(n.GetSiblingNode().(*FlattenNode), item)...)
			} else {
				// log.Printf("bloom filter (flatten node) between %v and %v is false\n", n.GetRange(), n.GetSiblingNode().GetRange())
				start_time := time.Now()
				cscr_res := n.CSCR.GetWithCache(item, t.CscCacheList)
				// cscr_res := n.CSCR.Get(item)
				t.TimeCounter.AddCSCRTime(start_time)
				// 防止 BloomFilter 假阳，需要回溯
				if len(cscr_res) == 0 && qp.IsPushedByBf {
					// log.Printf("    result of cscr is empty\n")
					queue.PushBack(NewQueryPlan(qp.ParentNode, nil, false, true))
					continue
				}
				for _, nid := range cscr_res {
					// log.Printf("    nid of cscr res: %v\n", nid)
					// 如果 nodeId 为 FlattenNode 的 id，需要进一步查 FlattenCSCR
					nid, _ := strconv.Atoi(nid)
					if nid == n.GetNid() {
						// log.Printf("    nid is equal to nid of flattenNode\n")
						res = append(res, t.searchFlattenCSCR(n, item)...)
					} else if nid == n.GetSiblingNode().GetNid() {
						// log.Printf("    nid is equal to nid of sibling of flattenNode\n")
						res = append(res, t.searchFlattenCSCR(n.GetSiblingNode().(*FlattenNode), item)...)
					} else {
						// log.Printf("    nid is child id\n")
						// 如果 nodeId 为叶子节点的 id，直接返回（需要判断是左节点还是右节点）
						leaf := n.GetChildById(nid)
						if leaf != nil {
							res = append(res, leaf)
						} else {
							leaf = n.GetSiblingNode().(*FlattenNode).GetChildById(nid)
							if leaf != nil {
								res = append(res, leaf)
							}
						}
					}
				}
			}
		}
	}
	return res
}

// 搜索 FlattenNode 的 FlattenCSCR，返回符合条件的节点
func (t *CSCTree) searchFlattenCSCR(node *FlattenNode, item string) []Node {
	// log.Printf("check flattenCSCR of FlattenNode: %v\n", node.GetRange())
	res := make([]Node, 0)
	start_time := time.Now()
	nidList := node.FlattenCSCR.GetWithCache(item, t.CscCacheList)
	// nidList := node.FlattenCSCR.Get(item)
	t.TimeCounter.AddCSCRTime(start_time)
	for _, nid := range nidList {
		nodeId, _ := strconv.Atoi(nid)
		child := node.GetChildById(nodeId)
		if child != nil {
			res = append(res, child)
		}
	}
	// log.Printf("result of flattenCSCR: %v\n", res)
	return res
}

func (t *CSCTree) AddWithBlockWithKLeafs(blockNumber int, txns []string, ctx *context.Context) bool {
	leafNode := NewLeafNode(blockNumber)
	leafNode.SetNid(t.GlobalNid)
	leafNode.SetSenderSet(block.NewAccountSetFromBlock(blockNumber, txns, true, leafNode.Nid))
	t.updateIndex(leafNode)
	t.GlobalNid++
	return t.AddWithKLeafs(leafNode, ctx)
}
