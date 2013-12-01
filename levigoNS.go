package abklevigoNS

import (
  "fmt"
  "strings"

  "github.com/jmhodges/levigo"

  "github.com/abhishekkr/levigoNS/leveldb"
)

type HashMap map[string]string

var (
  separator = ":"
)

func ReadNS(key string, db *levigo.DB) HashMap{
  var hmap HashMap
  hmap = make(HashMap)
  key = "key::" + key
  val := abkleveldb.GetVal(key, db)
  if val == "" { return hmap }
  children := strings.Split(val, ",")
  for _, child := range children {
    child_key := "val::" + strings.Split(child, "key::")[1]
    child_val := abkleveldb.GetVal(child_key, db)
    if child_val != "" { hmap[child] = child_val }
  }
  return hmap
}

func ReadNSRecursive(key string, db *levigo.DB) HashMap{
  var hmap HashMap
  hmap = make(HashMap)

  keyname := "key::" + key
  valname := "val::" + key
  keyname_val := abkleveldb.GetVal(keyname, db)
  valname_val := abkleveldb.GetVal(valname, db)
  if valname_val != "" { hmap[key] = valname_val }
  if keyname_val == "" { return hmap }
  children := strings.Split(keyname_val, ",")

  for _, child_val_as_key := range children {
    fmt.Println("child_val_as_key", child_val_as_key)
    child_key := strings.Split(child_val_as_key, "key::")[1]
    fmt.Println("child_key", child_key)
    inhmap := ReadNSRecursive(child_key, db)
    for inhmap_key, inhmap_val := range inhmap {
      hmap[inhmap_key] = inhmap_val
    }
  }

  return hmap
}

func IfChildExists(childKey string, parentValue string) bool {
  children := strings.Split(parentValue, ",")
  for _, child := range children {
    if child == childKey {
      return true
    }
  }
  return false
}

func AppendKey(parent string, child string, db *levigo.DB){
  parentKeyName := fmt.Sprintf("key::%s", parent)
  childKeyName := fmt.Sprintf("key::%s:%s", parent, child)

  val := abkleveldb.GetVal(parentKeyName, db)
  if val == "" {
    abkleveldb.PushKeyVal(parentKeyName, childKeyName, db)
  } else if IfChildExists(childKeyName, val) {
    abkleveldb.PushKeyVal(parentKeyName, val, db)
  } else {
    val = fmt.Sprintf("%s,%s", val, childKeyName)
    abkleveldb.PushKeyVal(parentKeyName, val, db)
  }
}

func CreateNS(key string, db *levigo.DB){
  splitIndex := strings.LastIndexAny(key, separator)
  if splitIndex >= 0 {
    parentKey := key[0:splitIndex]
    childKey := key[splitIndex+1:]

    AppendKey(parentKey, childKey, db)
    CreateNS(parentKey, db)
  }
}

func PushNS(key string, val string, db *levigo.DB) bool{
  CreateNS(key, db)
  key = "val::" + key
  return abkleveldb.PushKeyVal(key, val, db)
}
