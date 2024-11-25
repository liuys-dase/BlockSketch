package csctree

import (
	"strconv"
)

// EncodeNodeID 将节点ID和节点类型编码为字符串
func EncodeNodeID(vid int, isIntermediate bool) string {
	nodeType := "0" // 叶子节点
	if isIntermediate {
		nodeType = "1" // 中间节点
	}
	return strconv.Itoa(vid) + nodeType
}

// DecodeNodeID 解码编码的字符串为节点ID和节点类型
func DecodeNodeID(encoded string) (int, bool, error) {
	vidStr := encoded[:len(encoded)-1]      // 截取最后一位之前的部分为ID
	nodeTypeStr := encoded[len(encoded)-1:] // 最后一位为节点类型
	vid, err := strconv.Atoi(vidStr)
	if err != nil {
		return 0, false, err
	}
	nodeType := nodeTypeStr == "1" // 如果为1表示中间节点，否则为叶子节点
	return vid, nodeType, nil
}

// 比较两个 nodeid 的大小
func CompareNodeID(nodeID1, nodeID2 string) int {
	vid1, _ := strconv.Atoi(nodeID1[:len(nodeID1)-1])
	vid2, _ := strconv.Atoi(nodeID2[:len(nodeID2)-1])
	return vid1 - vid2
}

// 判断一个 nodeID 是否为中间节点
func IsInternalNode(nodeID string) bool {
	return nodeID[len(nodeID)-1:] == "1"
}

// 判断一个 nodeID 是否为叶子节点
func IsLeafNode(nodeID string) bool {
	return nodeID[len(nodeID)-1:] == "0"
}
