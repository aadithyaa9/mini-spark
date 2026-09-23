package funcs



import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"

)


type (
	Mapfn func(any) any
	Flatmapfn func(any) []any

	Filterfn func(any) bool
	Reducefn func(a , b any) any
	Keyfn func(any) any

	Partitionfn func([] any ) []any

	Createcombinerfn func(value any) any
	Mergevaluefn func(combiner , value any) any
	Mergecombinersfn func(a,b any) any 
)


var (
	mu sync.RWMutex
	registry = make(map[string]any)
)


func Register(name string , fn any ) error {
	if name == ""{
		return fmt.Errorf("empty name")
	}

	if fn == nil {
		return fmt.Errorf("nil function for %q" , name)

	}

	if !Issupported(fn){
		return fmt.Errorf("%q has unsuported signature , pls verify %T" , name , fn)
	}

	mu.Lock()
	defer mu.Unlock()

	if _,exists := registry[name] ; exists {
		return fmt.Errorf("%q it is registered already" , name)
	}

	registry[name] = fn
	return nil
}


func MustRegister(name string , fn any) {
	if err := Register(name , fn) ; err != nil {
		panic(err)
	}
}



func Issupported(fn any) bool {
	switch fn.(type) {
	case Mapfn , func(any) any :
		return true
	case Flatmapfn , func(any) []any :
		return true

	case Filterfn , func(any) bool :
		return true
	case Reducefn , func(a , b any) any :
		return true
	case Partitionfn , func([] any) []any:
		return true

	default:
		return false
	}
}

func Lookup(name string) (any ,error) {
	mu.RLock()
	fn ,ok := registry[name]
	defer mu.RUnlock()

	if !ok {
		return nil , fmt.Errorf ("%q not registered (known: %s)" , name , strings.Join(Names() , " , "))
	}

	return fn , nil
}



func LookupMap(name string) (Mapfn , error){

	fn , err := Lookup(name)

	if err != nil {
		return nil , err
	}

	switch f := fn.(type){
	case Mapfn :
		return f , nil

	case func(any) any :
		return f ,nil
	}

	return nil , fmt.Errorf("%q is %T , i want func(any) any" , name , fn)

}


func LookupFlatMap(name string) (Flatmapfn , error) {
	fn , err := Lookup(name)


	if err != nil {
		return nil , err
	}

	switch f := fn.(type) {
	case Flatmapfn :
		return f , nil

	case func(any) []any :
		return f , nil
	}

	return nil , fmt.Errorf("%q is %T , i want func(any) []any" , name , fn)
	
}


func LookupFilter(name string) (Filterfn , error) {
	fn  , err := Lookup(name) 
	if err != nil {
		return nil , err
	}


	switch f := fn.(type){
	case Filterfn :
		return f , nil
	

	case func(any) bool:
		return f, nil
	}

	return nil , fmt.Errorf("%q is %T , i want func(any) bool" , name , fn)
	

}


func LookupReduce(name string) (Reducefn  , error) {
	fn ,  err := Lookup(name)

	if err != nil{
		return nil , err
	}


	switch f := fn.(type){
	case Reducefn :
		return f, nil


	case func(a , b any) any :
		return f , nil
	}


	return nil , fmt.Errorf("funcs : %q is %T , want func(a , b any ) "  , name , fn)


}



func LookupPartition(name string) (Partitionfn , error ){

	fn , err := Lookup(name)


	
	if err != nil{
		return nil , err
	}


	switch f := fn.(type){
	case Partitionfn :
		return f , nil
	

	case func([]any) []any :
		return f , nil
	}

	return nil, fmt.Errorf("funcs: %q is %T, want func([]any) []any", name, fn)


}



func Names() []string{
	mu.RLock()
	defer mu.RUnlock()

	out := make([]string , 0 , len(registry))

	for k := range registry {
		out = append(out , k)
	}

	sort.Strings(out)
	return out
}


func Fingerprint() string {
	h := sha256.Sum256([]byte (strings.Join(Names() , "\x00")))
	return hex.EncodeToString(h[:8])

} 



func Reset() {
	mu.Lock()
	defer mu.Unlock()

	registry = make(map[string] any)
}