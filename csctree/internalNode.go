package csctree

import (
	"strconv"

	"github.com/liuys-dase/csc-tree/block"
	"github.com/liuys-dase/csc-tree/filter/basicfilter"
	"github.com/liuys-dase/csc-tree/filter/cscsketch"
)

type InternalNode struct {
	NodeType      NodeType // 节点类型
	LeftChild     Node     // 左孩子
	RightChild    Node     // 右孩子
	BloomFilter   *basicfilter.BloomFilter
	CSCR          *cscsketch.CSCR
	Level         int
	NodeRange     *block.BlockRange
	SenderSet     *block.AccountSet
	LeftChildFlag bool
	SiblingNode   Node
	Nid           int
}

// 两个 LeafNode 生成一个 InternalNode 时，两个 LeafNode 的 `冷点` 和 `边生成的冷点` 需要传递给 InternalNode
func NewInternalNode(leftChild Node, rightChild Node) *InternalNode {
	leftChild.SetLeftChildFlag(true)
	rightChild.SetLeftChildFlag(false)
	leftChild.SetSiblingNode(rightChild)
	return &InternalNode{
		NodeType:    INTERNAL,
		LeftChild:   leftChild,
		RightChild:  rightChild,
		BloomFilter: basicfilter.NewEmptyBloomFilter(),
		CSCR:        cscsketch.NewEmptyCSCR(),
		Level:       leftChild.GetLevel() + 1,
		NodeRange:   leftChild.GetRange().Merge(rightChild.GetRange()),
		SenderSet:   nil,
	}
}

func (i *InternalNode) SetNid(id int) {
	i.Nid = id
}

func (i *InternalNode) GetNid() int {
	return i.Nid
}

func (i *InternalNode) GetNodeType() NodeType {
	return i.NodeType
}

func (i *InternalNode) GetLevel() int {
	return i.Level
}

func (i *InternalNode) GetRange() *block.BlockRange {
	return i.NodeRange
}

func (i *InternalNode) GetSenderSet() *block.AccountSet {
	return i.SenderSet
}

func (i *InternalNode) SetBloomFilter(bf *basicfilter.BloomFilter) {
	i.BloomFilter = bf
}

func (i *InternalNode) SetSenderSet(as *block.AccountSet) {
	i.SenderSet = as
}

func (i *InternalNode) SetCSCR(cscr *cscsketch.CSCR) {
	i.CSCR = cscr
}

func (i *InternalNode) GetLeftChild() Node {
	return i.LeftChild
}

func (i *InternalNode) GetRightChild() Node {
	return i.RightChild
}

func (i *InternalNode) SetLeftChildFlag(isLeft bool) {
	i.LeftChildFlag = isLeft
}

func (i *InternalNode) IsLeftChild() bool {
	return i.LeftChildFlag
}

func (i *InternalNode) GetSiblingNode() Node {
	return i.SiblingNode
}

func (i *InternalNode) SetSiblingNode(n Node) {
	i.SiblingNode = n
}

// 转换为 RootNode
func (i *InternalNode) ToRootNode() *RootNode {
	return &RootNode{
		NodeType:   ROOT,
		LeftChild:  i.LeftChild,
		RightChild: i.RightChild,
		Level:      i.Level,
		NodeRange:  i.NodeRange,
		SenderSet:  i.SenderSet,
		Nid:        i.Nid,
	}
}

// 和 LeafNode 的 String() 方法相同
func (i *InternalNode) String() string {
	return "InternalNode{" +
		"NodeType: " + i.NodeType.String() + ", " +
		"IsLeftChild: " + strconv.FormatBool(i.LeftChildFlag) + ", " +
		"Level: " + strconv.Itoa(i.Level) + ", " +
		"NodeRange: " + i.NodeRange.String() +
		"}"
}
