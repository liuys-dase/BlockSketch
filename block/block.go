package block

// to be deleted
type Block struct {
	BlockNumber  string
	Transactions map[string][]*Transaction // sender -> []Transaction
}

func NewBlockFromString(blockNumber string, txnStrings []string) *Block {
	txs := make(map[string][]*Transaction)
	for _, txnString := range txnStrings {
		tx := NewTrasactionFromString(txnString)
		if _, ok := txs[tx.Sender]; !ok {
			txs[tx.Sender] = make([]*Transaction, 0)
		}
		txs[tx.Sender] = append(txs[tx.Sender], tx)
	}
	return &Block{
		BlockNumber:  blockNumber,
		Transactions: txs,
	}
}

func NewBlockFromBytes(blockNumber string, txnBytes []byte) *Block {
	txnStrings, _ := DecodeTransactions(txnBytes)
	txs := make(map[string][]*Transaction)
	for _, txnString := range txnStrings {
		tx := NewTrasactionFromString(txnString)
		if _, ok := txs[tx.Sender]; !ok {
			txs[tx.Sender] = make([]*Transaction, 0)
		}
		txs[tx.Sender] = append(txs[tx.Sender], tx)
	}
	return &Block{
		BlockNumber:  blockNumber,
		Transactions: txs,
	}
}

func (b *Block) GetSenderTransactions(sender string) []*Transaction {
	if txs, ok := b.Transactions[sender]; ok {
		return txs
	}
	return nil
}
