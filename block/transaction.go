package block

import (
	"strconv"
	"strings"
)

type Transaction struct {
	TxHash      string
	BlockNumber *BlockRange
	Sender      string
	Receiver    string
}

func NewTrasactionFromString(data string) *Transaction {
	txnSlice := strings.Split(data, ",")
	blockNumber, _ := strconv.Atoi(txnSlice[1])
	return &Transaction{
		TxHash:      txnSlice[0],
		BlockNumber: NewBlockRange(blockNumber, blockNumber),
		Sender:      txnSlice[2],
		Receiver:    txnSlice[3],
	}
}

func (t *Transaction) GetReceiver() string {
	return t.Receiver
}
