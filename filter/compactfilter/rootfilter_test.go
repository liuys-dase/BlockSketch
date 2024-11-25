package compactfilter

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/liuys-dase/csc-tree/csctree"
)

func TestRootFilter(t *testing.T) {
	// 读取数据
	lines, _ := ReadFileLines("/home/csc-tree/filter/compactfilter/rootdata.txt")

	// cold: 37721; hot: 14189; total: 51910
	cold_filter_capacity := 30000
	cold_filtet_fp_size := 5
	cold_filter_slot_num := 16
	rootFilter := NewRootFilterWithEstimation(
		cold_filter_capacity, cold_filtet_fp_size, cold_filter_slot_num, 30, 100, 2,
		14189, 0.01, 7, 32)
	count := 0
	// 插入数据
	for _, line := range lines {
		parts := strings.Split(line, ",")
		item := parts[0]
		nodeId := parts[1]
		ret := false
		if csctree.IsLeafNode(nodeId) {
			ret = rootFilter.SingleAdd(item, nodeId)
		} else {
			ret = rootFilter.MultiAdd(item, nodeId)
		}
		count++
		if !ret {
			fmt.Printf("insert failed at: %v\n", count)
			return
		}
	}
	fmt.Printf("Total Element Num: %v\n", count)
	fmt.Printf("Cold Filter Bucket Num: %v\n", rootFilter.ColdFilter.CSCs[0].NumBuckets)
	fmt.Printf("Utilization Count: %v\n", rootFilter.ColdFilter.CSCs[0].Utilization_count)
	fmt.Printf("Utilization Rate: %v\n", rootFilter.ColdFilter.CSCs[0].GetUtilizationRate())
	ans := rootFilter.Get("0xfa4e6790dfd8c05a8bca2924453055d3106ad6aa")
	fmt.Printf("Result Size: %v\n", len(ans))
}

func ReadFileLines(filePath string) ([]string, error) {
	var lines []string
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return lines, nil
}

func TestAnalyzeData(t *testing.T) {
	lines, _ := ReadFileLines("/home/csc-tree/filter/compactfilter/rootdata.txt")
	cold_count := 0
	hot_count := 0
	for _, line := range lines {
		parts := strings.Split(line, ",")
		nodeId := parts[1]
		if csctree.IsLeafNode(nodeId) {
			cold_count++
		} else {
			hot_count++
		}
	}
	fmt.Printf("cold_count: %d, hot_count: %d\n", cold_count, hot_count)
}
