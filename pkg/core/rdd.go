package core

import (
	"fmt"
	"strings"
	"sync/atomic"
)

type RDD interface {
	ID() int
	Name() string
	Partitions() []Partition
	Dependencies() []Dependency
	PreferredLocations(p Partition) []string
	Partitioner() Partitioner

	Compute(partition Partition, tc *TaskContext) (Iterator, error)
}

var rddIDCounter int64

func NextRDDID() int {
	return int(atomic.AddInt64(&rddIDCounter, 1))
}

type BaseRDD struct {
	id     int
	name   string
	deps   []Dependency
	level  StorageLevel
	PartFn Partitioner
}

func NewBaseRDD(name string, deps []Dependency) BaseRDD {
	return BaseRDD{
		id:   NextRDDID(),
		name: name,
		deps: deps,
	}
}

func (r *BaseRDD) ID() int {
	return r.id
}

func (r *BaseRDD) Name() string {
	return r.name
}

func (r *BaseRDD) Dependencies() []Dependency {
	return r.deps
}

func (r *BaseRDD) Partitioner() Partitioner {
	return r.PartFn
}

func (r *BaseRDD) PreferredLocations(Partition) []string {
	return nil
}

func (r *BaseRDD) StorageLevel() StorageLevel {
	return r.level
}

func (r *BaseRDD) SetStorageLevel(level StorageLevel) {
	r.level = level
}

type StorageLevel struct {
	UseDisk   bool
	UseMemory bool
}

var (
	StorageNone   = StorageLevel{}
	MemoryOnly    = StorageLevel{UseMemory: true}
	DiskOnly      = StorageLevel{UseDisk: true}
	MemoryAndDisk = StorageLevel{UseMemory: true, UseDisk: true}
)

func (l StorageLevel) IsCached() bool {
	return l.UseDisk || l.UseMemory
}

func (l StorageLevel) String() string {
	switch {
	case l.UseDisk && l.UseMemory:
		return "MemoryAndDisk"
	case l.UseDisk:
		return "DiskOnly"
	case l.UseMemory:
		return "MemoryOnly"
	default:
		return "StorageNone"
	}
}

type TaskContext struct {
	StageID     int
	PartitionID int
	Attempt     int
	Shuffle     ShuffleService
	Cache       CacheService
}

type ShuffleService interface {
	Writer(shuffleID, mapID int, part Partitioner, agg *Aggregator, mapSideCombine bool) (ShuffleWriter, error)

	Reader(shuffleID, reduceID int, agg *Aggregator, mapSideCombine bool) (Iterator, error)

	NumMaps(shuffleID int) int
}

type ShuffleWriter interface {
	Write(it Iterator) error
	Close() error
}

type CacheService interface {
	Get(blockID string) ([]any, bool)
	Put(blockID string, data []any, level StorageLevel) error
	Remove(blockID string)
	RemoveRDD(rddID int) int
}

func Lineage(r RDD) []RDD {
	seen := make(map[int]bool)
	var result []RDD
	var visit func(r RDD)
	visit = func(r RDD) {
		if seen[r.ID()] {
			return
		}
		seen[r.ID()] = true
		for _, dep := range r.Dependencies() {
			visit(dep.Parent())
		}
		result = append(result, r)
	}
	visit(r)
	return result
}

func ToDebugString(r RDD) string {
	var sb strings.Builder
	var visit func(RDD, int, bool)
	visit = func(r RDD, indent int, afterShuffle bool) {
		indentStr := strings.Repeat("  ", indent)
		marker := "|-"
		if afterShuffle {
			marker = "+-"
		}
		partitionerStr := ""
		if p := r.Partitioner(); p != nil {
			partitionerStr = fmt.Sprintf(" [partitioner: %T n=%d]", p, p.NumPartitions())
		}
		cacheStr := ""
		if br, ok := r.(interface{ StorageLevel() StorageLevel }); ok && br.StorageLevel().IsCached() {
			cacheStr = "[" + br.StorageLevel().String() + "]"
		}

		fmt.Fprintf(&sb, "%s%s %s[%d] partitions=%d%s%s\n", indentStr, marker, r.Name(), r.ID(), len(r.Partitions()), partitionerStr, cacheStr)
		for _, dep := range r.Dependencies() {
			visit(dep.Parent(), indent+1, dep.IsShuffle())
		}
	}
	visit(r, 0, false)
	return sb.String()
}

func RDDBlockID(rddID, partIdx int) string {
	return fmt.Sprintf("rdd_%d_part_%d", rddID, partIdx)
}
