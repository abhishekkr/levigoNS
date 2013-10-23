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
  val := abkleveldb.GetValues(key, db)
  if val == "" { return hmap }
  children := strings.Split(val, ",")
  for _, child := range children {
    childKey := "val::" + strings.Split(child, "key::")[1]
    hmap[child] = abkleveldb.GetValues(childKey, db)
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

  val := abkleveldb.GetValues(parentKeyName, db)
  if val == "" {
    abkleveldb.PushKeyVal(parentKeyName, childKeyName, db)
  } else if IfChildExists(childKeyName, val) {
    abkleveldb.PushKeyVal(parentKeyName, val, db)
  } else {
    val = fmt.Sprintf("%s,%s", val, childKeyName)
    abkleveldb.PushKeyVal(parentKeyName, val, db)
  }
  fmt.Printf("%s => %s\n", parentKeyName, val)
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
