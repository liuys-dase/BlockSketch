package block

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

// 使用 gob 编码交易数据
func EncodeTransactions(transactions []string) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(transactions)
	if err != nil {
		return nil, fmt.Errorf("failed to encode transactions: %w", err)
	}
	return buf.Bytes(), nil
}

// 使用 gob 解码交易数据
func DecodeTransactions(buf []byte) ([]string, error) {
	var transactions []string
	dec := gob.NewDecoder(bytes.NewReader(buf))
	err := dec.Decode(&transactions)
	if err != nil {
		return nil, fmt.Errorf("failed to decode transactions: %w", err)
	}
	return transactions, nil
}
