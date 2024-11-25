package cscsketch

import (
	"fmt"
	"testing"
)

func TestPartitionInsert(t *testing.T) {
	par := NewGlobalPartition(10, 1)
	keys := generateRandomStrings(8, 100, randomSeed())
	for _, key := range keys {
		par.Add(key)
	}
	par.PrintPartition()
	fmt.Printf("par = %d\n", par.GetPartitionId("hello"))
	fmt.Printf("par = %d\n", par.GetPartitionId("shit"))
	fmt.Printf("par = %d\n", par.GetPartitionId("hello"))
	fmt.Printf("par = %d\n", par.GetPartitionId("shit"))
}
