package core

import (
	"encoding/gob"
	"fmt"
	"hash/fnv"
)

type Partition interface {
	Index() int
	ID() string
}

type BasePartition struct {
	RDDID int
	Idx   int
}

func (p BasePartition) Index() int {
	return p.Idx
}

func (p BasePartition) ID() string {
	return fmt.Sprintf("rdd_%d_part_%d", p.RDDID, p.Idx)
}

type FileSplit struct {
	BasePartition
	Path   string
	Start  int64
	Length int64
}

type ShuffleReadPartition struct {
	BasePartition
	ShuffleID int
}

type UnionPartition struct {
	BasePartition
	ParentRDDIdx int
	Parent       Partition
}

func init() {
	gob.Register(BasePartition{})
	gob.Register(FileSplit{})
	gob.Register(ShuffleReadPartition{})
	gob.Register(KV{})
}

type Partitioner interface {
	NumPartitions() int
	GetPartition(key any) int
	Equals(other Partitioner) bool
}

type HashPartitioner struct {
	N int
}

func NewHashPartitioner(n int) *HashPartitioner {
	if n < 1 {
		n = 1
	}
	return &HashPartitioner{N: n}
}

func (h *HashPartitioner) NumPartitions() int { return h.N }

func (h *HashPartitioner) GetPartition(key any) int {
	return int(HashAny(key) % uint32(h.N))
}

func (h *HashPartitioner) Equals(other Partitioner) bool {
	o, ok := other.(*HashPartitioner)
	return ok && o.N == h.N
}

func HashAny(key any) uint32 {
	h := fnv.New32a()
	switch k := key.(type) {
	case string:
		h.Write([]byte(k))
	case []byte:
		h.Write(k)
	case int:
		writeInt(h, int64(k))

	case int32:
		writeInt(h, int64(k))
	case int64:
		writeInt(h, k)
	case bool:
		if k {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
	default:
		fmt.Fprintf(h, "%v", key)

	}
	return h.Sum32()
}

func writeInt(h interface{ Write([]byte) (int, error) }, v int64) {
	var b [8]byte
	for i := 0; i < 8; i++ {
		b[i] = byte(v >> (8 * i))
	}
	h.Write(b[:])
}
