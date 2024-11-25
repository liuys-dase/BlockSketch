package csctree

import (
	"strconv"

	"github.com/liuys-dase/csc-tree/block"
	"github.com/liuys-dase/csc-tree/filter/basicfilter"
	"github.com/liuys-dase/csc-tree/filter/cscsketch"
)

type RootNode struct {
	NodeType   NodeType // 节点类型
	LeftChild  Node     // 左孩子
	RightChild Node     // 右孩子
	Level      int
	NodeRange  *block.BlockRange
	SenderSet  *block.AccountSet
	Nid        int
}

func NewRootNode() *RootNode {
	return &RootNode{
		NodeType:  ROOT,
		SenderSet: nil,
	}
}

func (r *RootNode) SetNid(id int) {
	r.Nid = id
}

func (r *RootNode) GetNid() int {
	return r.Nid
}

func (r *RootNode) GetNodeType() NodeType {
	return r.NodeType
}

func (r *RootNode) GetLevel() int {
	return r.Level
}

func (r *RootNode) GetRange() *block.BlockRange {
	return r.NodeRange
}

func (r *RootNode) GetSenderSet() *block.AccountSet {
	return nil
}

func (r *RootNode) SetSenderSet(as *block.AccountSet) {
	r.SenderSet = as
}

func (r *RootNode) SetBloomFilter(bf *basicfilter.BloomFilter) {
}

func (r *RootNode) SetCSCR(cscr *cscsketch.CSCR) {
}

func (r *RootNode) GetLeftChild() Node {
	return r.LeftChild
}

func (r *RootNode) GetRightChild() Node {
	return r.RightChild
}

func (r *RootNode) SetLeftChildFlag(isLeft bool) {
}

func (r *RootNode) IsLeftChild() bool {
	return false
}

func (r *RootNode) GetSiblingNode() Node {
	return nil
}

func (r *RootNode) SetSiblingNode(n Node) {
}

func (r *RootNode) String() string {
	return "RootNode{" +
		"NodeType: " + r.NodeType.String() + ", " +
		// "BloomFilter: " + l.BloomFilter.String() + ", " +
		// "CSCR: " + l.CSCR.String() + ", " +
		"Level: " + strconv.Itoa(r.Level) + ", " +
		"NodeRange: " + r.NodeRange.String() +
		"}"
}
