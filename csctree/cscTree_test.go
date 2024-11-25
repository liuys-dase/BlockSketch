package csctree

import (
	"fmt"
	"testing"

	"github.com/liuys-dase/csc-tree/context"
)

func cscTreeContext() *context.Context {
	ctx, _ := context.NewContext("../config.ini")
	fmt.Printf("MaxLevel: %d\n", ctx.Config.CSCTreeConfig.MaxLevel)
	return ctx
}

func TestCscTreeAdd(t *testing.T) {
	ctx := cscTreeContext()
	cscTree := NewCSCTree(ctx)
	fmt.Println(cscTree.Add(NewLeafNode(1), ctx))
	fmt.Println(cscTree.Add(NewLeafNode(2), ctx))
	fmt.Println(cscTree.Add(NewLeafNode(3), ctx))
	fmt.Println(cscTree.Add(NewLeafNode(4), ctx))
	fmt.Println(cscTree.Add(NewLeafNode(5), ctx))
	// 输出 Root 及其指针的地址
	fmt.Printf("Root: %v\n", cscTree.Root)
	fmt.Printf("Size of Queue: %d, Queue: %v\n", cscTree.queue.Size(), cscTree.queue)
	nodes := cscTree.BFS()
	for _, node := range nodes {
		fmt.Printf("%v\n", node)
	}
}
