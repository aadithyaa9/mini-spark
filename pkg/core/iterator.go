package core

import (
	"fmt"

)


type Iterator interface {
	Next() bool 
	Value() any
	Err() error
	Close() error
}



type SliceIterator struct {
	data[] any
	pos int
}


func NewSliceIterator(data[] any) Iterator {
	return  &SliceIterator{data : data ,pos : -1}
}


func (it *SliceIterator) Next() bool{
	it.pos++
	return it.pos  < len(it.data)
}

func (it *SliceIterator) Value() any {
	return it.data[it.pos]
}


func (it  *SliceIterator) Err() error {
	return nil
}

func (it *SliceIterator) Close() error {
	it.data = nil
	return nil
}




type EmptyIterator struct {}

func NewEmptyIterator() Iterator { return EmptyIterator{}}

func (EmptyIterator) Next() bool   { return false }
func (EmptyIterator) Value() any   { return nil }
func (EmptyIterator) Err() error   { return nil }
func (EmptyIterator) Close() error { return nil }


type ErrorIterator struct {err error}

func NewErrorIterator(err error) Iterator {return &ErrorIterator{err : err}}

func (e *ErrorIterator) Next() bool   { return false }
func (e *ErrorIterator) Value() any   { return nil }
func (e *ErrorIterator) Err() error   { return e.err }
func (e *ErrorIterator) Close() error { return nil }

type MapIterator struct {
	parent Iterator 
	fn func(any) any
	cur any
}



func NewMapIterator(parent Iterator ,  fn func(any) any) Iterator {
	return &MapIterator{parent : parent, fn : fn}
}



func (it  *MapIterator) Next() bool {
	if !it.parent.Next(){
		return false
	}

	it.cur = it.fn(it.parent.Value())
	return true
}


func(it *MapIterator) Value() any {return it.cur}
func(it *MapIterator) Err() error {return it.parent.Err()}
func(it *MapIterator) Close() error {return it.parent.Close()}


type FilterIterator struct {
	parent Iterator
	fn func(any) bool
	cur any
}




func NewFilterIterator (parent Iterator , fn func(any) bool) Iterator {
	return &FilterIterator{parent : parent , fn : fn}
}

func (it *FilterIterator) Next() bool {
	for it.parent.Next(){
		v := it.parent.Value()
		if it.fn(v){
			it.cur = v
			return true
		}
	}
	return false
}

func (it *FilterIterator) Value() any   { return it.cur }
func (it *FilterIterator) Err() error   { return it.parent.Err() }
func (it *FilterIterator) Close() error { return it.parent.Close() }


type FlatMapIterator struct {
	parent Iterator
	fn     func(any) []any
	buf    []any
	bufPos int
}

func NewFlatMapIterator(parent Iterator, fn func(any) []any) Iterator {
	return &FlatMapIterator{parent: parent, fn: fn, bufPos: -1}
}

func (it *FlatMapIterator) Next() bool {
	for {
		it.bufPos++
		if it.bufPos < len(it.buf) {
			return true
		}
		if !it.parent.Next() {
			return false
		}
		it.buf = it.fn(it.parent.Value())
		it.bufPos = -1
	}
}
func (it *FlatMapIterator) Value() any   { return it.buf[it.bufPos] }
func (it *FlatMapIterator) Err() error   { return it.parent.Err() }
func (it *FlatMapIterator) Close() error { return it.parent.Close() }




type ChainIterator struct {
	parts []Iterator
	idx int
	err error
}


func NewChainIterator (parts ...Iterator) Iterator {
	return &ChainIterator{parts : parts}
}


func (it *ChainIterator) Next() bool {
	for it.idx < len(it.parts) {
		if it.parts[it.idx].Next(){
			return true
		}

		if err:= it.parts[it.idx].Err() ; err != nil {
			it.err = err
			return false
		}
		it.idx++
	}
	return false
}



func(it *ChainIterator) Value() any {return it.parts[it.idx].Value() }
func (it *ChainIterator) Err() error {return it.err}
func (it *ChainIterator ) Close() error {
	var first error
	for _ ,p := range it.parts{
		if err := p.Close() ; err != nil && first == nil {
			first = err
		}
	}
	return first
}





func Collect(it Iterator ) ([]any , error){
	defer it.Close()
	var out []any
	for it.Next(){
		out = append(out , it.Value())

	}
	return out , it.Err()
}


func CountRecords(it Iterator) (int64  , error){
	defer it.Close()
	var n int64
	for it.Next(){
		n++
	}
	return n , it.Err()
}




func TakeN (it Iterator , n int) ([]any , error){
	defer it.Close()
	out := make([]any , 0 , n )
	for len(out) < n && it.Next() {
		out = append(out , it.Value())
	}
	return out , it.Err()
}





type KV struct {
	Key any
	Value any
}


func (kv KV) String() string {
	return fmt.Sprintf("(%v , %v)" , kv.Key , kv.Value)
}


func AsKV(v any) (KV ,error){
	kv , ok := v.(KV)
	if !ok{
		return KV{}, fmt.Errorf("core: expected core.KV record, got %T (did you forget to map to pairs first?)", v)
	}
	return kv , nil
}