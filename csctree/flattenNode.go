package csctree

import (
	"math"
	"strconv"

	"github.com/liuys-dase/csc-tree/block"
	"github.com/liuys-dase/csc-tree/filter/basicfilter"
	"github.com/liuys-dase/csc-tree/filter/cscsketch"
)

type FlattenNode struct {
	NodeType      NodeType    // 节点类型
	Children      []*LeafNode // 子节点
	BloomFilter   *basicfilter.BloomFilter
	CSCR          *cscsketch.CSCR
	Level         int
	NodeRange     *block.BlockRange
	SenderSet     *block.AccountSet
	LeftChildFlag bool
	SiblingNode   Node
	Nid           int
	AccountMap    *AccountMap // flatten node 的新属性
	FlattenCSCR   *cscsketch.CSCR
	TmpAccountMap *AccountMap // 指存放 children 中的热点
}

// 将多个 LeafNode 合并成一个 FlattenNode
func NewFlattenNode(children []*LeafNode) *FlattenNode {
	accountMap := NewAccountMap()
	for _, child := range children {
		accountMap.AddSenderSet(child.GetSenderSet())
		child.SetSenderSet(nil)
	}
	// 配置 blockRange
	start := children[0].GetRange().Start
	end := children[len(children)-1].GetRange().End
	for _, child := range children {
		if child.GetRange().Start < start {
			start = child.GetRange().Start
		}
		if child.GetRange().End > end {
			end = child.GetRange().End
		}
	}

	return &FlattenNode{
		NodeType:      FLATTEN,
		Children:      children,
		BloomFilter:   basicfilter.NewEmptyBloomFilter(),
		CSCR:          cscsketch.NewEmptyCSCR(),
		Level:         int(math.Log2(float64(len(children)))) + 1,
		NodeRange:     block.NewBlockRange(start, end),
		SenderSet:     nil,
		AccountMap:    accountMap,
		FlattenCSCR:   cscsketch.NewEmptyCSCR(),
		TmpAccountMap: nil,
	}
}

func (i *FlattenNode) SetNid(id int) {
	i.Nid = id
}

func (i *FlattenNode) GetNid() int {
	return i.Nid
}

func (i *FlattenNode) GetNodeType() NodeType {
	return i.NodeType
}

func (i *FlattenNode) GetLevel() int {
	return i.Level
}

func (i *FlattenNode) GetRange() *block.BlockRange {
	return i.NodeRange
}

func (i *FlattenNode) GetSenderSet() *block.AccountSet {
	return i.SenderSet
}

func (i *FlattenNode) SetBloomFilter(bf *basicfilter.BloomFilter) {
	i.BloomFilter = bf
}

func (i *FlattenNode) SetSenderSet(as *block.AccountSet) {
	i.SenderSet = as
}

func (i *FlattenNode) SetCSCR(cscr *cscsketch.CSCR) {
	i.CSCR = cscr
}

func (i *FlattenNode) GetLeftChild() Node {
	return nil
}

func (i *FlattenNode) GetRightChild() Node {
	return nil
}

func (i *FlattenNode) SetLeftChildFlag(isLeft bool) {
	i.LeftChildFlag = isLeft
}

func (i *FlattenNode) IsLeftChild() bool {
	return i.LeftChildFlag
}

func (i *FlattenNode) GetSiblingNode() Node {
	return i.SiblingNode
}

func (i *FlattenNode) SetSiblingNode(n Node) {
	i.SiblingNode = n
}

func (i *FlattenNode) SetChildren(children []*LeafNode) {
	i.Children = children
}

func (i *FlattenNode) GetChildById(nid int) *LeafNode {
	for _, child := range i.Children {
		if child.GetNid() == nid {
			return child
		}
	}
	return nil
}

// 和 LeafNode 的 String() 方法相同
func (i *FlattenNode) String() string {
	return "FlattenNode{" +
		"NodeType: " + i.NodeType.String() + ", " +
		"IsLeftChild: " + strconv.FormatBool(i.LeftChildFlag) + ", " +
		"Level: " + strconv.Itoa(i.Level) + ", " +
		"NodeRange: " + i.NodeRange.String() +
		"}"
}

// 保证 AccountMap 中，每个地址对应的 NidList 中的 Nid 是唯一的
type UniqueNidList struct {
	// string: nid
	NidList map[int]bool
}

func NewUniqueNidList() *UniqueNidList {
	return &UniqueNidList{
		NidList: make(map[int]bool),
	}
}
func (l *UniqueNidList) ToString() string {
	ret := "[ "
	for nid := range l.NidList {
		ret += strconv.Itoa(nid) + " "
	}
	ret += "]"
	return ret
}

func (l *UniqueNidList) Insert(nid int) {
	l.NidList[nid] = true
}

func (l *UniqueNidList) Union(l2 *UniqueNidList) *UniqueNidList {
	res := NewUniqueNidList()
	for nid := range l.NidList {
		res.NidList[nid] = true
	}
	for nid := range l2.NidList {
		res.NidList[nid] = true
	}
	return res
}

type AccountMap struct {
	// address -> nidList
	Map map[string]*UniqueNidList
}

func NewAccountMap() *AccountMap {
	return &AccountMap{
		Map: make(map[string]*UniqueNidList),
	}
}

func (m *AccountMap) ToString() string {
	ret := "{ "
	for addr, nidList := range m.Map {
		ret += addr + ": " + nidList.ToString() + " "
	}
	ret += "}"
	return ret
}

func (m *AccountMap) AddSenderSet(senderSet *block.AccountSet) {
	for addr, nid := range senderSet.Accounts {
		// 如果 addr 尚未添加，则创建一个新的 UniqueNidList
		if _, ok := m.Map[addr]; !ok {
			m.Map[addr] = NewUniqueNidList()
		}
		m.Map[addr].NidList[nid] = true
	}
}

// 提取两个 AccountMap 中公共的 addr
func (m *AccountMap) Intersect(m2 *AccountMap) []string {
	var res []string
	for addr := range m.Map {
		if _, ok := m2.Map[addr]; ok {
			res = append(res, addr)
		}
	}
	return res
}

// 创建一个新的 AccountMap，包含两个 AccountMap 中的所有数据
func (m *AccountMap) Union(m2 *AccountMap) *AccountMap {
	res := NewAccountMap()
	for addr, nidList := range m.Map {
		res.Map[addr] = nidList
	}
	for addr, nidList := range m2.Map {
		if _, ok := res.Map[addr]; !ok {
			res.Map[addr] = nidList
		} else {
			res.Map[addr] = res.Map[addr].Union(nidList)
		}
	}
	return res
}

func (m *AccountMap) ToAccountSet() *block.AccountSet {
	as := block.NewAccountSet(len(m.Map))
	for addr, nidList := range m.Map {
		for nid := range nidList.NidList {
			as.Accounts[addr] = nid
		}
	}
	return as
}

// 用于从 intersection 中提取元素，但此时 accountMap 中每个元素的 nid 应该只会有一个，因此只需要返回 AccountSet
func (m *AccountMap) BatchGetWithDelete(keys []string) *AccountMap {
	res := NewAccountMap()
	for _, key := range keys {
		if v, ok := m.Map[key]; ok {
			res.Map[key] = v
			delete(m.Map, key)
		}
	}
	return res
}

func (m *AccountMap) Size() int {
	ret := 0
	for _, v := range m.Map {
		ret += len(v.NidList)
	}
	return ret
}
