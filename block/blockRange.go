package block

import "fmt"

type BlockRange struct {
	Start int
	End   int
}

func NewBlockRange(start int, end int) *BlockRange {
	return &BlockRange{
		Start: start,
		End:   end,
	}
}

func (nr *BlockRange) Intersect(nr2 *BlockRange) bool {
	return nr.Start <= nr2.End && nr.End >= nr2.Start
}

func (nr *BlockRange) GetStart() int {
	return nr.Start
}

func (nr *BlockRange) GetEnd() int {
	return nr.End
}

func (nr *BlockRange) GetRange() (int, int) {
	return nr.Start, nr.End
}

func (nr *BlockRange) Size() int {
	return nr.End - nr.Start + 1
}

func (nr *BlockRange) Merge(nr2 *BlockRange) *BlockRange {
	if nr.End+1 == nr2.Start {
		return NewBlockRange(nr.Start, nr2.End)
	}
	return nil
}

// 重写 String 方法，用于输出
func (nr *BlockRange) String() string {
	return fmt.Sprintf("[%d,%d]", nr.Start, nr.End)
}

// 将 BlockRange 转换为字符串
func (nr *BlockRange) ToString() string {
	return fmt.Sprintf("[%d,%d]", nr.Start, nr.End)
}
