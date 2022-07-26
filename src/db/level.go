package db

import (
	"fmt"
	"sort"
	"sync"
)

var NotFound = fmt.Errorf("key not find")
type level struct {
	mu sync.RWMutex
	files FileStates

	level int
	lastS int //当前最后文件的sequence
}

func (l *level) HighLevelGet(key string)(string, error) {
	l.mu.RLock()
	fs := l.files
	l.mu.RUnlock()
	idx := sort.Search(len(fs), func(i int) bool {
		return fs[i].startKey < key
	})
	if idx >= len(fs) {
		return "", NotFound
	}
	f := fs[idx]
	if f.startKey <= key && f.endKey >= key {
		return f.Get(key)
	}
	return "", NotFound
}

func (l *level) Level0Get(key string)(string ,error) {
	l.mu.RLock()
	fs := l.files
	l.mu.RUnlock()
	if len(fs) == 0 {
		return "", NotFound
	}
	for i := len(fs)-1; i >= 0 ; i-- {
		if fs[i].startKey <= key && fs[i].endKey >= key {
			return fs[i].Get(key)
		}
	}
	return "", NotFound
}

func (l *level) LastSequence() (i int) {
	l.mu.Lock()
	i = l.lastS
	l.lastS++
	l.mu.Unlock()
	return
}

func (l *level) AppendFileState(f *FileState) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.files = append(l.files, f)
}
