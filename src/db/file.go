package db

import (
	"fmt"
	"os"
	"path"
)


type FileStates []*FileState

func (f FileStates) Len() int{
	return len(f)
}
func (f FileStates)Less(i, j int) bool {
	return f[i].startKey < f[j].endKey
}

func (f FileStates) Swap(i, j int) {
	f[i], f[j] = f[j], f[i]
}

type FileState struct {
	f *os.File
	data []byte

	startKey, endKey string
	size int // bytes number
	length int // key number

	sequence int
	level int
}

func NewTmpFileState(level, sequence int, dirPath string) (*FileState, error) {
	fileName := fmt.Sprintf("%d-%d.lsm.tmp", level, sequence)
	fullPath := path.Join(dirPath, fileName)
	f, err := os.OpenFile(fullPath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	fs := new(FileState)
	fs.f = f
	fs.level = level
	fs.sequence = sequence
	return fs, nil
}

func (f *FileState) Get(key string) (string, error) {

}

func (f *FileState) Write(key, value string) {
	f.f.WriteString(key)
	f.f.WriteString("\t")
	f.f.WriteString(value)
	f.f.WriteString("\n")
	f.size += len(key) + len(value) + 2
	f.length += 1
}

func (f *FileState) WriteHeader() {

}

func (f *FileState) WriteFooter() {

}

func (f *FileState) RenameFile() {
	oldPath := f.f.Name()
	newPath := oldPath[:len(oldPath)-4]
	os.Rename(oldPath, newPath)
}


