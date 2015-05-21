package main

import (
	"fmt"
	"runtime"

	"github.com/jmhodges/levigo"

	lns "github.com/abhishekkr/levigoNS"
	abkleveldb "github.com/abhishekkr/levigoNS/leveldb"
)

var (
	separator = ":"
)

func read(key string, db *levigo.DB) string {
	val := abkleveldb.GetVal(key, db)
	fmt.Printf("for %s get %s\n", key, val)
	return val
}

func exampleNS(db *levigo.DB) {
	runtime.GOMAXPROCS(runtime.NumCPU())

	fmt.Println("add some data first for a,a:1,a:2,a:1:2,a:2:1,a:3,a:1:1 ~")
	lns.PushNS("a", "A", db)
	lns.PushNS("a:1", "A1", db)
	lns.PushNS("a:2", "A2", db)
	lns.PushNS("a:1:2", "A12", db)
	lns.PushNS("a:2:1", "A21", db)
	lns.PushNS("a:3", "A3", db)
	lns.PushNS("a:1:1", "A11", db)
	lns.PushNS("b:2:1", "A11", db)

	fmt.Println("read some data now ~")
	read("val::a", db)
	read("val::a:1", db)
	read("val::a:2", db)
	read("val::a:1:2", db)
	read("val::a:2:1", db)
	read("val::a:3", db)
	read("val::a:1:1", db)

	fmt.Println("super keys~")
	read("key::a", db)
	read("key::a:1", db)
	read("key::a:2", db)
	read("key::b", db)
	read("key::b:2", db)

	var hmap map[string]string
	hmap = make(map[string]string)
	hmap = lns.ReadNS("key::a", db)
	for k, v := range hmap {
		fmt.Printf("%s => %s\n", k, v)
	}
	hmap = lns.ReadNS("key::c", db)
	for k, v := range hmap {
		fmt.Printf("%s => %s", k, v)
	}

	fmt.Println("read/delete~")
	fmt.Printf("Recursive under a: %v\n", lns.ReadNSRecursive("a", db))
	fmt.Println("before del a:1:2 ~ ", abkleveldb.GetVal("key::a:1", db))
	lns.DeleteNSRecursive("a:1:2", db)
	fmt.Println("after del of a:1:2 ~ ", abkleveldb.GetVal("key::a:1", db))
	fmt.Printf("Recursive under a: %v\n", lns.ReadNSRecursive("a", db))
	fmt.Println("before del a ~ ", abkleveldb.GetVal("key::a", db))
	lns.DeleteNSRecursive("a", db)
	fmt.Println("after del a ~ ", abkleveldb.GetVal("key::a", db))
	fmt.Printf("Recursive under a: %v\n", lns.ReadNSRecursive("a", db))
	fmt.Printf("Recursive under b: %v\n", lns.ReadNSRecursive("b", db))
	fmt.Println("before del of b ~", abkleveldb.GetVal("key::b", db))
	lns.DeleteNS("b", db)
	fmt.Println("after del of b ~ ", abkleveldb.GetVal("key::b", db))
	fmt.Println("after del b:2 ~ ", abkleveldb.GetVal("key::b:2", db))
	fmt.Println("after del b:2:1 ~ ", abkleveldb.GetVal("key::b:2:1", db))
	fmt.Printf("Recursive under b: %v\n", lns.ReadNSRecursive("b", db))
}

func main() {
	var db *levigo.DB
	db = abkleveldb.CreateDB("/tmp/LevelDB02")
	exampleNS(db)
}
