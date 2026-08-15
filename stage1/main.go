package main 


//  Single goal as of now , we need to make sure to count and reshuffle all the occurence of the word across document
//  and also across machine , first we need to make sure that it works in this simplest system , inotder to avoid future confusions.
import (
	"fmt"
	"sort"
	"strings"
)

// KV is used as a Map Reuce


type KV struct {
	Key  string
	Value string
}



// Map function

// This function takes one line of text and emit zero or more KV pairs.

// For word count : every word in the line becomes a sepereate KV pair with the word as the key and "1" as the value.


// why 1 -> remember in future 

// Because map runs on different machines . machine a sees a word 3 times , machine b also sees the same word for 4 times , and no one knows about each other 

// the reducer adds them all up and combines them into a single KV pair with the word as the key and the total count as the value.

// This makes the map reduce parrelizable .

func mapfn (line string ) []KV {


	var pairs []KV

	for _ , word := range strings.Fields(line) {

		word = strings.ToLower(strings.Trim(word , `.,!?;:"'()-`))
		if word  == ""{
			continue
		}

		pairs = append(pairs , KV{Key: word , Value: "1"})
	}
	return pairs

}

// Shuffle function


// Shuffle groups all values for the same key together 


// this is the imp according to me as , without this , the reducer can't work properly

// it needs all values for a particular key in one place - but they came from different map calls running on different lines or in different machine (when you try to run a 1tb file on different machine using distributed map reduce )


// shuffle is the gathering phase . everything scattered by map , gets assembled here


// in real world systems , shuffle means writing intermediate files to disk 
// then transferring it via network between map and sorting by key 
 

// so shuffle is the bottle neck in production systems

func shuffle(pairs []KV) map[string][]string {
	grouped := make(map[string][]string)
	for _, kv := range pairs {
		grouped[kv.Key] = append(grouped[kv.Key], kv.Value)
	}
	return grouped
}




// Reduce function

// takes one key and all its values , and producess a single result
// for word count , it sums up all the values and returns a single KV pair with the word as the key and the total count as the value.

// why not just sum the values as integers ? 

// because in real system , map reduce values are not always integers , they can be any type of data , so we need to keep it generic and flexible


// the reducer only sees one key at a time . 
// it never knows whats happening for other keys 
// this makes the reducer parrelizable across machines.


func reducefn(key string  , values []string) string {
	return fmt.Sprintf("%d", len(values))
}



func main(){


	input := []string{
		"abu is what my friend call me at my place" , 
		"this happened because i trimmed my beard like that" , 
		"i am not sure if i can do this or not" ,
		"can you please tell me what is the best way to do this" ,
		"what is the best way to do this" ,
		"you can do this by following the steps mentioned in the document" ,
		"this is the worst way to do this" ,

	}

	for i , line := range input {
		fmt.Printf("Line %d : %s\n" , i+1 , line)
	}
	fmt.Println()
	var allPairs []KV

	for i , line  := range input {
		pairs := mapfn(line)
		fmt.Printf("Map (line %d) -> %v" , i , pairs)

		allPairs = append(allPairs, pairs...)
	}


	fmt.Printf("\nTotal pairs after map: %d\n\n", len(allPairs))


	grouped := shuffle(allPairs)

	fmt.Println("Shuffled")

	keys := make([]string , 0 , len(grouped))
	for k := range grouped{
		keys = append(keys , k)

	}

	sort.Strings(keys)

	for _ , key := range keys {
		fmt.Printf("  %q → %v\n", key, grouped[key])
	}

	fmt.Println("Shuffled done")


	result := make(map[string]string)

	for key , values := range grouped {
		result[key] = reducefn(key , values)
	}
	
	fmt.Println("\nFinal Result : ")

	for _ , key := range keys {
		fmt.Printf("  %q → %s\n", key, result[key])
		}

}