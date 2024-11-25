package csctree

import (
	"strconv"

	"github.com/liuys-dase/csc-tree/block"
	"github.com/liuys-dase/csc-tree/filter/basicfilter"
	"github.com/liuys-dase/csc-tree/filter/cscsketch"
)

type NodeType int

const (
	LEAF     NodeType = iota + 1 // 叶子结点
	INTERNAL                     // 内部节点
	ROOT                         // 根节点
	FLATTEN                      // 多个叶子结点合并的节点
)

func (n NodeType) String() string {
	switch n {
	case LEAF:
		return "LEAF"
	case INTERNAL:
		return "INTERNAL"
	case ROOT:
		return "ROOT"
	case FLATTEN:
		return "FLATTEN"
	default:
		return "UNKNOWN"
	}
}

type Node interface {
	// 获取节点类型
	GetNodeType() NodeType
	// 获取层高
	GetLevel() int
	// 获取范围
	GetRange() *block.BlockRange
	// 获取交易列表
	GetSenderSet() *block.AccountSet
	// 设置交易列表
	SetSenderSet(as *block.AccountSet)
	// 设置布隆过滤器
	SetBloomFilter(bf *basicfilter.BloomFilter)
	// 设置 CSCR
	SetCSCR(cscr *cscsketch.CSCR)
	// 重写 String() 方法
	String() string
	// 获取左孩子
	GetLeftChild() Node
	// 获取右孩子
	GetRightChild() Node
	// 设置左孩子标识
	SetLeftChildFlag(isLeft bool)
	// 判断是否是左孩子
	IsLeftChild() bool
	// 获取兄弟节点
	GetSiblingNode() Node
	// 设置兄弟节点
	SetSiblingNode(n Node)
	// 获取节点 ID
	GetNid() int
	// 设置节点 ID
	SetNid(id int)
}

type LeafNode struct {
	NodeType      NodeType                 // 节点类型
	BloomFilter   *basicfilter.BloomFilter // 布隆过滤器(边/热点)
	CSCR          *cscsketch.CSCR          // CSC(冷点)
	Level         int                      // 节点所在层级
	NodeRange     *block.BlockRange        // 节点负责的数据范围
	SenderSet     *block.AccountSet        // 每个叶子结点对应一个区块
	LeftChildFlag bool                     // 判断是否是左孩子
	SiblingNode   Node                     // 兄弟节点的指针
	Nid           int
}

// 创建一个新的 LeafNode, 并且 senderset 为空
func NewLeafNode(singleRange int) *LeafNode {
	return &LeafNode{
		NodeType:    LEAF,
		BloomFilter: basicfilter.NewEmptyBloomFilter(),
		CSCR:        cscsketch.NewEmptyCSCR(),
		Level:       1,
		NodeRange:   block.NewBlockRange(singleRange, singleRange),
		SenderSet:   nil,
	}
}

// 根据 block 数据创建 LeafNode
// func NewLeafNodeFromBlock(blockNumber int, txnStrings []string) *LeafNode {
// 	return &LeafNode{
// 		NodeType:    LEAF,
// 		BloomFilter: filter.NewEmptyBloomFilter(),
// 		CSCR:        filter.NewEmptyCSCR(),
// 		Level:       1,
// 		NodeRange:   block.NewBlockRange(blockNumber, blockNumber),
// 		SenderSet:   block.NewAccountSetFromBlock(blockNumber, txnStrings, true),
// 	}
// }

func (l *LeafNode) SetNid(id int) {
	l.Nid = id
}

func (l *LeafNode) GetNid() int {
	return l.Nid
}

func (l *LeafNode) GetNodeType() NodeType {
	return l.NodeType
}

func (l *LeafNode) GetLevel() int {
	return l.Level
}

func (l *LeafNode) GetRange() *block.BlockRange {
	return l.NodeRange
}

func (l *LeafNode) GetSenderSet() *block.AccountSet {
	return l.SenderSet
}

func (l *LeafNode) SetSenderSet(as *block.AccountSet) {
	l.SenderSet = as
}

func (l *LeafNode) SetBloomFilter(bf *basicfilter.BloomFilter) {
	l.BloomFilter = bf
}

func (l *LeafNode) SetCSCR(cscr *cscsketch.CSCR) {
	l.CSCR = cscr
}

func (l *LeafNode) GetLeftChild() Node {
	return nil
}

func (l *LeafNode) GetRightChild() Node {
	return nil
}

func (l *LeafNode) SetLeftChildFlag(isLeft bool) {
	l.LeftChildFlag = isLeft
}

func (l *LeafNode) IsLeftChild() bool {
	return l.LeftChildFlag
}

func (l *LeafNode) GetSiblingNode() Node {
	return l.SiblingNode
}

func (l *LeafNode) SetSiblingNode(n Node) {
	l.SiblingNode = n
}

// 输出 LeafNode 的每个属性
func (l *LeafNode) String() string {
	return "LeafNode{" +
		"NodeType: " + l.NodeType.String() + ", " +
		"IsLeftChild: " + strconv.FormatBool(l.LeftChildFlag) + ", " +
		"Level: " + strconv.Itoa(l.Level) + ", " +
		"NodeRange: " + l.NodeRange.String() +
		"}"
}
