package db

import (
	"os"
	"sync"
)

type QueryType byte

const (
	INSERT QueryType = iota
	DELETE
	SELECT
)

type Entry struct {
	typ QueryType
	index int
	data []byte
}

type WALFile struct {
	f *os.File
	size int
}

func NewWALFile(p string) *WALFile {
	wal := new(WALFile)
	f, _ := os.OpenFile(p, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	wal.f = f
	return wal
}
type WAL struct {
	mu sync.Mutex

	curr *WALFile
	old []*WALFile

	MaxSize int // 单个wal日志最大的文件大小
}

// 写入entry
// 布局：type + len(data) + data
// 其中len(data) int16表示 2 byte
func (w *WAL) Write(entries []Entry) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var bs []byte
	var k int
	for _, entry := range entries {
		if k + 3 + len(entry.data) + w.curr.size > w.MaxSize {
			w.curr.f.Write(bs)
			w.old = append(w.old, w.curr)
			w.curr =
			w.curr.size += k + 3 + len(entry.data)
		} else {

		}


	}

}






