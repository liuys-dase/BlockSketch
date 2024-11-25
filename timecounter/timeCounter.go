package timecounter

import (
	"log"
	"time"
)

type BlockSketchTimeCounter struct {
	BFTime   int64
	CSCRTime int64
}

func NewBlockSketchTimeCounter() *BlockSketchTimeCounter {
	return &BlockSketchTimeCounter{
		BFTime:   0,
		CSCRTime: 0,
	}
}

func (t *BlockSketchTimeCounter) AddBFTime(start_time time.Time) {
	elapsed := time.Since(start_time)
	t.BFTime += elapsed.Microseconds()
}

func (t *BlockSketchTimeCounter) AddCSCRTime(start_time time.Time) {
	elapsed := time.Since(start_time)
	t.CSCRTime += elapsed.Microseconds()
}

func (t *BlockSketchTimeCounter) Print() {
	log.Printf("BFTime: %v, CSCRTime: %v", t.BFTime, t.CSCRTime)
}

func (t *BlockSketchTimeCounter) Clear() {
	t.BFTime = 0
	t.CSCRTime = 0
}

func (t *BlockSketchTimeCounter) GetCSCRTime() int64 {
	return t.CSCRTime
}
