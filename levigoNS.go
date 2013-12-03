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


/*
Reads all direct child values in a given NameSpace
For e.g.:
  given keys a, a:b, a:b:1, a:b:2, a:b:2:3
  reads for a:b:1, a:b:2 if queried for a:b
*/
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


/*
Reads all values belonging to tree below given NameSpace
For e.g.:
  given keys a, a:b, a:b:1, a:b:2, a:b:2:3
  reads for a:b:1, a:b:2, a:b:2:3 if queried for a:b
*/
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
    child_key := strings.Split(child_val_as_key, "key::")[1]
    inhmap := ReadNSRecursive(child_key, db)
    for inhmap_key, inhmap_val := range inhmap {
      hmap[inhmap_key] = inhmap_val
    }
  }

  return hmap
}


/*
Given all full child keynames of a given NameSpace reside as string separated
by a comma(","). This method checks for a given keyname being a child keyname
for provided for group string of all child keynames.
Return:
  true if given keyname is present as child in group-val of child keynames
  false if not
*/
func IfChildExists(childKey string, parentValue string) bool {
  children := strings.Split(parentValue, ",")
  for _, child := range children {
    if child == childKey {
      return true
    }
  }
  return false
}


/*
Given a parent keyname and child keyname,
updates the group-val for child keynames of a parent keyname as required.
*/
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


/*
Given a keyname takes care of updating entry of all trail of NameSpaces.
*/
func CreateNS(key string, db *levigo.DB){
  splitIndex := strings.LastIndexAny(key, separator)
  if splitIndex >= 0 {
    parentKey := key[0:splitIndex]
    childKey := key[splitIndex+1:]

    AppendKey(parentKey, childKey, db)
    CreateNS(parentKey, db)
  }
}


/*
Standard function to feed in NameSpace entries given namespace key and val.
*/
func PushNS(key string, val string, db *levigo.DB) bool{
  CreateNS(key, db)
  key = "val::" + key
  return abkleveldb.PushKeyVal(key, val, db)
}


/*
Update key's presence from it's parent's  group-val of child key names.
*/
func UnrootNS(key string, db *levigo.DB){
  split_index := strings.LastIndexAny(key, separator)
  if split_index < 0 { return }
  parent_key := key[0:split_index]
  self_keyname := fmt.Sprintf("key::%s" , key)
  parent_keyname := fmt.Sprintf("key::%s" , parent_key)
  parent_keyname_val := abkleveldb.GetVal(parent_keyname, db)
  if parent_keyname_val == "" { return }
  parent_keyname_val_elem := strings.Split(parent_keyname_val, ",")

  _tmp_array := make([]string, len(parent_keyname_val_elem))
  _tmp_array_idx := 0
  for _, elem := range parent_keyname_val_elem {
    if elem != self_keyname {
      _tmp_array[_tmp_array_idx] = elem
      _tmp_array_idx += 1
    }
  }
  parent_keyname_val = strings.Join(_tmp_array[0:len(_tmp_array)-1], ":")
  if parent_keyname_val == "" {
    UnrootNS(parent_key, db)
  }

  abkleveldb.PushKeyVal(parent_keyname, parent_keyname_val, db)
}


/*
Standard function to directly delete a child key-val and unroot it from parent.
*/
func DeleteNSKey(key string, db *levigo.DB){
  self_val := "val::" + key
  abkleveldb.DelKey(self_val, db)

  key = "key::" + key
  abkleveldb.DelKey(key, db)

  UnrootNS(key, db)
}


/*
Standard function to delete a namespace with all direct children and unroot it.
*/
func DeleteNS(key string, db *levigo.DB){
  self_val := "val::" + key
  abkleveldb.DelKey(self_val, db)

  key = "key::" + key
  val := abkleveldb.GetVal(key, db)
  abkleveldb.DelKey(key, db)

  if val == "" { return }
  children := strings.Split(val, ",")
  for _, child_key := range children {
    child_val := "val::" + strings.Split(child_key, "key::")[1]
    abkleveldb.DelKey(child_key, db)
    abkleveldb.DelKey(child_val, db)
  }

  UnrootNS(key, db)
}


/*
Standard function to delete a namespace with all children below and unroot it.
*/
func DeleteNSRecusrive(key string, db *levigo.DB){
  keyname := "key::" + key
  valname := "val::" + key
  keyname_val := abkleveldb.GetVal(keyname, db)
  abkleveldb.DelKey(keyname, db)
  abkleveldb.DelKey(valname, db)

  if keyname_val == "" { return }
  children := strings.Split(keyname_val, ",")
  for _, child_val_as_key := range children {
    child_key := strings.Split(child_val_as_key, "key::")[1]
    DeleteNSRecusrive(child_key, db)
  }

  UnrootNS(key, db)
}
