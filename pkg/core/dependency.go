package core

type Dependency interface {
	Parent() RDD
	IsShuffle() bool
}

type NarrowDependency interface {
	Dependency
	GetParents(childIndex int) []int
}

type OneToOneDependency struct {
	parent RDD
}

func NewOneToOneDependency(parent RDD) *OneToOneDependency {
	return &OneToOneDependency{parent: parent}
}

func (d *OneToOneDependency) Parent() RDD {
	return d.parent
}

func (d *OneToOneDependency) IsShuffle() bool {
	return false
}

func (d *OneToOneDependency) GetParents(childIndex int) []int {
	return []int{childIndex}
}

type RangeDependency struct {
	parent   RDD
	inStart  int
	outStart int
	length   int
}

func NewRangeDependency(parent RDD, inStart, outStart, length int) *RangeDependency {
	return &RangeDependency{
		parent:   parent,
		inStart:  inStart,
		outStart: outStart,
		length:   length,
	}
}

func (d *RangeDependency) Parent() RDD {
	return d.parent
}

func (d *RangeDependency) IsShuffle() bool {
	return false
}

func (d *RangeDependency) GetParents(childIndex int) []int {
	if childIndex >= d.outStart && childIndex < d.outStart+d.length {
		return []int{childIndex - d.outStart + d.inStart}
	}
	return nil
}

type ShuffleDependency struct {
	parent         RDD
	ShuffleID      int
	Part           Partitioner
	Agg            *Aggregator
	MapSideCombine bool
}

func NewShuffleDependency(parent RDD, shuffleID int, p Partitioner, agg *Aggregator, mapSideCombine bool) *ShuffleDependency {
	return &ShuffleDependency{
		parent:         parent,
		ShuffleID:      shuffleID,
		Part:           p,
		Agg:            agg,
		MapSideCombine: mapSideCombine,
	}
}

func (d *ShuffleDependency) Parent() RDD {
	return d.parent
}

func (d *ShuffleDependency) IsShuffle() bool {
	return true
}

type Aggregator struct {
	CreateCombiner string
	MergeValue     string
	MergeCombiners string
}
