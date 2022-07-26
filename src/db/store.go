package db

import (
	"sort"
	"sync"
)

type Store interface {
	Get(key string) (string, error)
	SaveSnapshot(map[string]string) error
}

type FileStore struct {
	sync.RWMutex

	level0 level
	levels []level

	dir string
}

func (fs *FileStore) Get(key string) (string, error) {
	v, err := fs.level0.Level0Get(key)
	if err == nil {
		return v, err
	}else if err != NotFound {
		return "", err
	}
	for i := 0; i < len(fs.levels); i++ {
		v, err = fs.levels[i].HighLevelGet(key)
		if err == nil {
			return v, err
		}else if err != NotFound {
			return "", err
		}
	}
	return "", NotFound
}

func (fs *FileStore)SaveSnapshot(snapshot map[string]string) error{
	keys := make([]string, 0, len(snapshot))
	for k := range snapshot {
		keys = append(keys, k)
	}
	sequence := fs.level0.LastSequence()
	tmpFile, err := NewTmpFileState(0, sequence, fs.dir)
	if err != nil {
		return err
	}
	sort.Strings(keys)
	for _, key := range keys {
		tmpFile.Write(key, snapshot[key])
	}
	tmpFile.WriteFooter()
	tmpFile.RenameFile() // 从temp 文件转换为正式文件
	fs.level0.AppendFileState(tmpFile)
	return nil
}





