package block

import (
	"strconv"
	"strings"
)

type AccountSet struct {
	Accounts map[string]int
}

func NewAccountSet(num int) *AccountSet {
	return &AccountSet{
		Accounts: make(map[string]int, num),
	}
}

func NewAccountSetFromBlock(blockNumber int, txns []string, isSender bool, nid int) *AccountSet {
	as := NewAccountSet(len(txns))
	for _, txn := range txns {
		txnSlice := strings.Split(txn, ",")
		var account string
		if isSender {
			account = txnSlice[2]
		} else {
			account = txnSlice[3]
		}
		as.Accounts[account] = nid
	}
	return as
}

func (as *AccountSet) ToString() string {
	ret := "{ "
	for addr, nid := range as.Accounts {
		ret += addr + ": " + strconv.Itoa(nid) + " "
	}
	ret += "}"
	return ret
}

func (as *AccountSet) Add(key string, value int) {
	as.Accounts[key] = value
}

// 删除元素
func (as *AccountSet) Delete(account string) {
	delete(as.Accounts, account)
}

// 批量删除元素
func (as *AccountSet) BatchDelete(accounts []string) {
	for _, account := range accounts {
		as.Delete(account)
	}
}

// 两个 AccountSet 的交集
func (as *AccountSet) Intersect(as2 *AccountSet) []string {
	// log.Printf("Intersect: %v, %v\n", as, as2)
	var res []string
	for k := range as.Accounts {
		if _, ok := as2.Accounts[k]; ok {
			res = append(res, k)
		}
	}
	return res
}

// 两个 AccountSet 的并集，返回一个新的 AccountSet
func (as *AccountSet) Union(as2 *AccountSet) *AccountSet {
	res := NewAccountSet(len(as.Accounts) + len(as2.Accounts))
	for k, v := range as.Accounts {
		res.Accounts[k] = v
	}
	for k, v := range as2.Accounts {
		res.Accounts[k] = v
	}
	return res
}

// 批量获取元素,同时删除原有元素
func (as *AccountSet) BatchGetWithDelete(keys []string) map[string]int {
	res := make(map[string]int)
	for _, key := range keys {
		if v, ok := as.Accounts[key]; ok {
			res[key] = v
		}
	}
	as.BatchDelete(keys)
	return res
}

func (as *AccountSet) GetAccount() map[string]int {
	return as.Accounts
}

func (as *AccountSet) GetSize() int {
	return len(as.Accounts)
}
