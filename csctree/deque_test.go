package csctree

import (
	"fmt"
	"testing"
)

func TestDeque(t *testing.T) {
	d := NewDeque()
	d.PushFront(1)
	d.PushFront(2)
	d.PushFront(3)
	d.PushFront(4)
	fmt.Print(d.KBack(1))
	fmt.Print(d.KBack(2))
	fmt.Print(d.KBack(3))
	fmt.Print(d.KBack(4))
	fmt.Print(d.KBack(5))
}
