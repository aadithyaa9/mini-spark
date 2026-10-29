package rdds

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/aadithyaa9/mini-spark/pkg/core"
)

type TextFileRDD struct {
	core.BaseRDD
	path   string
	splits []core.FileSplit
}

func NewTextFile(path string, minPartitions int) (*TextFileRDD, error) {
	if minPartitions < 1 {
		minPartitions = 1
	}

	info, err := os.Stat(path)

	if err != nil {
		return nil, fmt.Errorf("rdds : textfile : %q : %v", path, err)

	}

	if info.IsDir() {
		return nil, fmt.Errorf("rdds : textfile : %q is a directory", path)
	}

	r := &TextFileRDD{
		BaseRDD: core.NewBaseRDD("TextFileRDD", nil),
		path:    path,
	}

	r.splits = computeSplits(r.ID(), path, info.Size(), minPartitions)

	return r, nil
}

func computeSplits(rddID int, path string, size int64, n int) []core.FileSplit {
	if size == 0 {
		return []core.FileSplit{{
			BasePartition: core.BasePartition{RDDID: rddID, Idx: 0},
			Path:          path,
		}}
	}

	const minSplitBytes = 1 << 10

	if size/int64(n) < minSplitBytes {
		n = int(size/minSplitBytes) + 1
	}

	splits := make([]core.FileSplit, 0, n)
	chunk := size / int64(n)
	var start int64

	for i := 0; i < n; i++ {
		length := chunk

		if i == n-1 {
			length = size - start
		}

		splits = append(splits, core.FileSplit{
			BasePartition: core.BasePartition{RDDID: rddID, Idx: i},
			Path:          path,
			Start:         start,
			Length:        length,
		})

		start += length
	}

	return splits

}

func (r *TextFileRDD) Partitions() []core.Partition {

	out := make([]core.Partition, len(r.splits))

	for i, s := range r.splits {

		out[i] = s
	}

	return out
}

func (r *TextFileRDD) PreferredLocations(core.Partition) []string {
	return nil
}

func (r *TextFileRDD) Compute(p core.Partition, _ *core.TaskContext) (core.Iterator, error) {
	split, ok := p.(core.FileSplit)

	if !ok {
		return nil, fmt.Errorf("rdds : TextFileRDD got %T , want FileSplit", p)
	}

	f, err := os.Open(split.Path)

	if err != nil {
		return nil, err
	}

	needSkip := false

	if split.Start > 0 {
		var prev [1]byte

		if _, err := f.ReadAt(prev[:], split.Start-1); err != nil {
			f.Close()
			return nil, err
		}

		needSkip = prev[0] != '\n'
	}

	if _, err := f.Seek(split.Start, io.SeekStart); err != nil {
		f.Close()

		return nil, err
	}

	it := &lineIterator{
		f:      f,
		reader: bufio.NewReaderSize(f, 64<<10),
		pos:    split.Start,
		end:    split.Start + split.Length,
	}

	if needSkip {
		if err := it.skipPartialLine(); err != nil {
			f.Close()
			return nil, err
		}

	}
	return it, nil
}

type lineIterator struct {
	f      *os.File
	reader *bufio.Reader
	pos    int64
	end    int64
	cur    string
	err    error
	done   bool
}

func (it *lineIterator) skipPartialLine() error {

	line, err := it.reader.ReadBytes('\n')

	it.pos += int64(len(line))

	if err != nil && len(line) == 0 {
		it.done = true
	}

	return nil
}

func (it *lineIterator) Next() bool {

	if it.done || it.pos >= it.end {
		return false
	}

	line, err := it.reader.ReadBytes('\n')

	if len(line) == 0 {
		it.done = true

		if err != nil && err.Error() != "EOF" {
			it.err = err
		}
		return false
	}

	it.pos += int64(len(line))

	n := len(line)
	if n > 0 && line[n-1] == '\n' {
		n--
	}

	if n > 0 && line[n-1] == '\r' {
		n--
	}

	it.cur = string(line[:n])
	return true
}

func (it *lineIterator) Value() any { return it.cur }

func (it *lineIterator) Err() error { return it.err }

func (it *lineIterator) Close() error {

	if it.f != nil {
		err := it.f.Close()
		it.f = nil
		return err
	}
	return nil
}
