package rdds

import (
	"fmt"

	"github.com/aadithyaa9/mini-spark/pkg/core"
	"github.com/aadithyaa9/mini-spark/pkg/funcs"
)

type ParallelCollectionRDD struct {
	core.BaseRDD
	slices [][]any
}

func NewParallelCollectionRDD(data []any, numSlices int) *ParallelCollectionRDD {
	if numSlices < 1 {
		numSlices = 1
	}

	r := &ParallelCollectionRDD{
		BaseRDD: core.NewBaseRDD("ParallelCollectionRDD", nil),
		slices:  sliceData(data, numSlices),
	}
	return r
}

func sliceData(data []any, n int) [][]any {
	out := make([][]any, n)
	total := len(data)
	base, extra := total/n, total%n
	start := 0
	for i := 0; i < n; i++ {
		size := base
		if i < extra {
			size++
		}
		out[i] = data[start : start+size]
		start += size
	}
	return out
}

func (r *ParallelCollectionRDD) Partitions() []core.Partition {
	parts := make([]core.Partition, len(r.slices))
	for i := range r.slices {
		parts[i] = core.BasePartition{RDDID: r.ID(), Idx: i}

	}

	return parts
}

func (r *ParallelCollectionRDD) Compute(partition core.Partition, _ *core.TaskContext) (core.Iterator, error) {
	return core.NewSliceIterator(r.slices[partition.Index()]), nil
}

type MappedRDD struct {
	core.BaseRDD
	parent   core.RDD
	funcName string
}

func NewMappedRDD(parent core.RDD, funcName string) *MappedRDD {
	return &MappedRDD{
		BaseRDD:  core.NewBaseRDD("MappedRDD", []core.Dependency{core.NewOneToOneDependency(parent)}),
		parent:   parent,
		funcName: funcName,
	}
}

func (r *MappedRDD) Partitions() []core.Partition {
	return r.parent.Partitions()
}

func (r *MappedRDD) PreferredLocations(partition core.Partition) []string {
	return r.parent.PreferredLocations(partition)
}

func (r *MappedRDD) Compute(partition core.Partition, tc *core.TaskContext) (core.Iterator, error) {
	fn, err := funcs.LookupMap(r.funcName)
	if err != nil {
		return nil, err
	}

	iter, err := ComputeOrCache(r.parent, partition, tc)
	if err != nil {
		return nil, err
	}

	return core.NewMapIterator(iter, fn), nil
}

type FlatMappedRDD struct {
	core.BaseRDD
	parent   core.RDD
	funcName string
}

func NewFlatMappedRDD(parent core.RDD, funcName string) *FlatMappedRDD {
	return &FlatMappedRDD{
		BaseRDD:  core.NewBaseRDD("FlatMappedRDD", []core.Dependency{core.NewOneToOneDependency(parent)}),
		parent:   parent,
		funcName: funcName,
	}
}

func (r *FlatMappedRDD) Partitions() []core.Partition {
	return r.parent.Partitions()
}

func (r *FlatMappedRDD) PreferredLocations(partition core.Partition) []string {
	return r.parent.PreferredLocations(partition)
}

func (r *FlatMappedRDD) Compute(partition core.Partition, tc *core.TaskContext) (core.Iterator, error) {
	fn, err := funcs.LookupFlatMap(r.funcName)
	if err != nil {
		return nil, err
	}

	iter, err := ComputeOrCache(r.parent, partition, tc)
	if err != nil {
		return nil, err
	}

	return core.NewFlatMapIterator(iter, fn), nil
}

type FilteredRDD struct {
	core.BaseRDD
	parent   core.RDD
	funcName string
}

func NewFilteredRDD(parent core.RDD, funcName string) *FilteredRDD {
	r := &FilteredRDD{
		BaseRDD:  core.NewBaseRDD("FilteredRDD", []core.Dependency{core.NewOneToOneDependency(parent)}),
		parent:   parent,
		funcName: funcName,
	}

	r.PartFn = parent.Partitioner()
	return r
}

func (r *FilteredRDD) Partitions() []core.Partition {
	return r.parent.Partitions()
}

func (r *FilteredRDD) PreferredLocations(partition core.Partition) []string {
	return r.parent.PreferredLocations(partition)
}

func (r *FilteredRDD) Compute(partition core.Partition, tc *core.TaskContext) (core.Iterator, error) {
	fn, err := funcs.LookupFilter(r.funcName)
	if err != nil {
		return nil, err
	}

	iter, err := ComputeOrCache(r.parent, partition, tc)
	if err != nil {
		return nil, err
	}

	return core.NewFilterIterator(iter, fn), nil
}

type MappedPartitionsRDD struct {
	core.BaseRDD
	parent               core.RDD
	funcName             string
	preservePartitioning bool
}

func NewMappedPartitionsRDD(parent core.RDD, funcName string, preservePartitioning bool) *MappedPartitionsRDD {
	r := &MappedPartitionsRDD{
		BaseRDD:              core.NewBaseRDD("MappedPartitionsRDD", []core.Dependency{core.NewOneToOneDependency(parent)}),
		parent:               parent,
		funcName:             funcName,
		preservePartitioning: preservePartitioning,
	}
	if preservePartitioning {
		r.PartFn = parent.Partitioner()
	}
	return r
}

func (r *MappedPartitionsRDD) Partitions() []core.Partition {
	return r.parent.Partitions()
}

func (r *MappedPartitionsRDD) PreferredLocations(partition core.Partition) []string {
	return r.parent.PreferredLocations(partition)
}

func (r *MappedPartitionsRDD) Compute(partition core.Partition, tc *core.TaskContext) (core.Iterator, error) {
	fn, err := funcs.LookupPartition(r.funcName)
	if err != nil {
		return nil, err
	}

	iter, err := ComputeOrCache(r.parent, partition, tc)

	if err != nil {
		return nil, err
	}
	records, err := core.Collect(iter)
	if err != nil {
		return nil, err
	}

	return core.NewSliceIterator(fn(records)), nil
}

type UnionRDD struct {
	core.BaseRDD
	parents []core.RDD
}

func NewUnionRDD(parents []core.RDD) *UnionRDD {
	deps := make([]core.Dependency, 0, len(parents))
	out := 0
	for _, p := range parents {
		n := len(p.Partitions())
		deps = append(deps, core.NewRangeDependency(p, 0, out, n))
		out += n
	}
	return &UnionRDD{
		BaseRDD: core.NewBaseRDD("UnionRDD", deps),
		parents: parents,
	}

}

func (r *UnionRDD) Partitions() []core.Partition {
	var out []core.Partition
	index := 0
	for pi, p := range r.parents {
		for _, part := range p.Partitions() {
			out = append(out, core.UnionPartition{
				BasePartition: core.BasePartition{RDDID: r.ID(), Idx: index},
				ParentRDDIdx:  pi,
				Parent:        part,
			})
			index++
		}
	}
	return out
}

func (r *UnionRDD) Compute(partition core.Partition, tc *core.TaskContext) (core.Iterator, error) {
	up, ok := partition.(core.UnionPartition)
	if !ok {
		return nil, fmt.Errorf("expected UnionPartition, got %T", partition)
	}

	return ComputeOrCache(r.parents[up.ParentRDDIdx], up.Parent, tc)

}

func ComputeOrCache(rdd core.RDD, partition core.Partition, tc *core.TaskContext) (core.Iterator, error) {
	br, ok := rdd.(interface{ StorageLevel() core.StorageLevel })
	if !ok || !br.StorageLevel().IsCached() || tc == nil || tc.Cache == nil {
		return rdd.Compute(partition, tc)
	}

	blockID := core.RDDBlockID(rdd.ID(), partition.Index())
	if records, hit := tc.Cache.Get(blockID); hit {
		return core.NewSliceIterator(records), nil
	}

	iter, err := rdd.Compute(partition, tc)
	if err != nil {
		return nil, err
	}

	records, err := core.Collect(iter)
	if err != nil {
		return nil, err
	}

	if err := tc.Cache.Put(blockID, records, br.StorageLevel()); err != nil {
		return core.NewSliceIterator(records), nil
	}

	return core.NewSliceIterator(records), nil
}
