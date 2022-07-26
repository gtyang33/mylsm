package db

import (
	"fmt"
	"sync"
	"time"
)

var DBClosed = fmt.Errorf("db is closed")

type DB struct {
	mu sync.RWMutex

	*DBOption

	kv map[string]string
	snapshot map[string]string
	last time.Time
	files Store

	closed bool
	closeCh chan struct{}
}

type DBOption struct {
	snapshotTimeOut time.Duration
	snapshotSize int
}

func NewDB(option *DBOption) *DB {
	db := new(DB)
	db.DBOption = option
	return db
}

func (db *DB) Put(key, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return DBClosed
	}
	db.kv[key] = value
	return nil
}

func (db *DB)Get(key string) (string, error) {
	db.mu.RLock()
	if db.closed {
		return "", DBClosed
	}
	m := db.kv
	snapshot := db.snapshot
	db.mu.RUnlock()
	if v, ok := m[key]; ok {
		return v, nil
	}
	if v, ok := snapshot[key]; ok {
		return v, nil
	}
	return db.files.Get(key)
}

func (db *DB) Close() {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.closed = true
	close(db.closeCh)
}

func (db *DB) doSnapshot() {
	tick := time.NewTicker(db.DBOption.snapshotTimeOut)
	for {
		select {
		case <-tick.C:
			db.mu.Lock()
			if len(db.kv) < db.DBOption.snapshotSize{
				db.mu.Unlock()
				continue
			}
			db.snapshot = db.kv
			db.kv = make(map[string]string)
			db.mu.Unlock()
			db.files.SaveSnapshot(db.snapshot)
			db.mu.Lock()
			db.snapshot = nil
			db.mu.Unlock()
		case <-db.closeCh:
			return
		}
	}
}








