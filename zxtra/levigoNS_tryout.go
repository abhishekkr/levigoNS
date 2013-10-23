package main

import (
  "fmt"
  "runtime"

  "github.com/jmhodges/levigo"

  lns "github.com/abhishekkr/levigoNS"
  abkleveldb "github.com/abhishekkr/levigoNS/leveldb"
)

func Read(key string, db *levigo.DB) string{
  val := abkleveldb.GetValues(key, db)
  return val
}

func ExampleNS(db *levigo.DB) {
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
  Read("val::a", db)
  Read("val::a:1", db)
  Read("val::a:2", db)
  Read("val::a:1:2", db)
  Read("val::a:2:1", db)
  Read("val::a:3", db)
  Read("val::a:1:1", db)

  fmt.Println("super keys~")
  Read("key::a", db)
  Read("key::a:1", db)
  Read("key::a:2", db)
  Read("key::b", db)
  Read("key::b:2", db)

  var hmap map[string]string
  hmap = make(map[string]string)
  hmap = lns.ReadNS("key::a", db)
  for k,v := range hmap {
    fmt.Printf("%s => %s\n", k, v)
  }
  hmap = lns.ReadNS("key::c", db)
  for k,v := range hmap {
    fmt.Printf("%s => %s", k, v)
  }
}

func main(){
  var db *levigo.DB
  db = abkleveldb.CreateDB("/tmp/LevelDB02")
  ExampleNS(db)
}
