package abklevigoNS

import (
	"fmt"
	"strings"

	"github.com/jmhodges/levigo"

	golhashmap "github.com/abhishekkr/gol/golhashmap"
	"github.com/abhishekkr/levigoNS/leveldb"
)

var (
	NamespaceSeparator = ":"
)

/*
Reads all direct child values in a given NameSpace
For e.g.:
  given keys a, a:b, a:b:1, a:b:2, a:b:2:3
  reads for a:b:1, a:b:2 if queried for a:b
*/
func ReadNS(key string, db *levigo.DB) golhashmap.HashMap {
	var hmap golhashmap.HashMap
	hmap = make(golhashmap.HashMap)
	key = "key::" + key
	val := abkleveldb.GetVal(key, db)
	if val == "" {
		return hmap
	}
	children := strings.Split(val, ",")
	for _, child := range children {
		child_key := "val::" + strings.Split(child, "key::")[1]
		child_val := abkleveldb.GetVal(child_key, db)
		if child_val != "" {
			hmap[child] = child_val
		}
	}
	return hmap
}

/*
Reads all values belonging to tree below given NameSpace
For e.g.:
  given keys a, a:b, a:b:1, a:b:2, a:b:2:3
  reads for a:b:1, a:b:2, a:b:2:3 if queried for a:b
*/
func ReadNSRecursive(key string, db *levigo.DB) golhashmap.HashMap {
	var hmap golhashmap.HashMap
	hmap = make(golhashmap.HashMap)

	keyname := "key::" + key
	valname := "val::" + key
	keyname_val := abkleveldb.GetVal(keyname, db)
	valname_val := abkleveldb.GetVal(valname, db)
	if valname_val != "" {
		hmap[key] = valname_val
	}
	if keyname_val == "" {
		return hmap
	}
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
func ifChildExists(childKey string, parentValue string) bool {
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
func appendKey(parent string, child string, db *levigo.DB) bool {
	parentKeyName := fmt.Sprintf("key::%s", parent)
	childKeyName := fmt.Sprintf("key::%s:%s", parent, child)
	status := true

	val := abkleveldb.GetVal(parentKeyName, db)
	if val == "" {
		if !abkleveldb.PushKeyVal(parentKeyName, childKeyName, db) {
			status = false
		}
	} else if ifChildExists(childKeyName, val) {
		if !abkleveldb.PushKeyVal(parentKeyName, val, db) {
			status = false
		}
	} else {
		val = fmt.Sprintf("%s,%s", val, childKeyName)
		if !abkleveldb.PushKeyVal(parentKeyName, val, db) {
			status = false
		}
	}
	return status
}

/*
Given a keyname takes care of updating entry of all trail of NameSpaces.
*/
func CreateNS(key string, db *levigo.DB) bool {
	splitIndex := strings.LastIndexAny(key, NamespaceSeparator)
	if splitIndex >= 0 {
		parentKey := key[0:splitIndex]
		childKey := key[splitIndex+1:]

		if appendKey(parentKey, childKey, db) {
			return CreateNS(parentKey, db)
		} else {
			return false
		}
	}
	return true
}

/*
Standard function to feed in NameSpace entries given namespace key and val.
*/
func PushNS(key string, val string, db *levigo.DB) bool {
	CreateNS(key, db)
	key = "val::" + key
	return abkleveldb.PushKeyVal(key, val, db)
}

/*
Update key's presence from it's parent's  group-val of child key names.
*/
func UnrootNS(key string, db *levigo.DB) bool {
	status_parent_unroot, status_parent_update := true, true
	split_index := strings.LastIndexAny(key, NamespaceSeparator)
	if split_index < 0 {
		return true
	}
	parent_key := key[0:split_index]
	self_keyname := fmt.Sprintf("key::%s", key)
	parent_keyname := fmt.Sprintf("key::%s", parent_key)
	parent_keyname_val := abkleveldb.GetVal(parent_keyname, db)
	if parent_keyname_val == "" {
		return true
	}
	parent_keyname_val_elem := strings.Split(parent_keyname_val, ",")

	_tmp_array := make([]string, len(parent_keyname_val_elem))
	_tmp_array_idx := 0
	for _, elem := range parent_keyname_val_elem {
		if elem != self_keyname {
			if elem == "" {
				continue
			}
			_tmp_array[_tmp_array_idx] = elem
			_tmp_array_idx += 1
		}
	}

	if _tmp_array_idx > 1 {
		parent_keyname_val = strings.Join(_tmp_array[0:_tmp_array_idx-1], ",")
	} else {
		parent_keyname_val = _tmp_array[0]
	}

	if parent_keyname_val == "" {
		status_parent_unroot = UnrootNS(parent_key, db)
	}

	status_parent_update = abkleveldb.PushKeyVal(parent_keyname, parent_keyname_val, db)

	return status_parent_unroot && status_parent_update
}

/*
Standard function to directly delete a child key-val and unroot it from parent.
*/
func DeleteNSKey(key string, db *levigo.DB) bool {
	defer UnrootNS(key, db)
	self_val := "val::" + key
	if abkleveldb.DelKey(self_val, db) {
		keyname := "key::" + key
		if abkleveldb.DelKey(keyname, db) {
			return true
		}
	}
	return false
}

/*
Private function to delete direct children of any keyname
*/
func deleteNSChildren(val string, db *levigo.DB) bool {
	status := true
	children := strings.Split(val, ",")
	for _, child_key := range children {
		child_val := "val::" + strings.Split(child_key, "key::")[1]
		status = status && abkleveldb.DelKey(child_key, db)
		status = status && abkleveldb.DelKey(child_val, db)
	}
	return status
}

/*
Standard function to delete a namespace with all direct children and unroot it.
*/
func DeleteNS(key string, db *levigo.DB) bool {
	defer UnrootNS(key, db)
	self_val := "val::" + key
	if abkleveldb.DelKey(self_val, db) {
		keyname := "key::" + key
		val := abkleveldb.GetVal(keyname, db)
		if abkleveldb.DelKey(keyname, db) {
			if val == "" {
				return true
			}
			return deleteNSChildren(val, db)
		}
	}
	return false
}

/*
Private function to delete recursive children of any keyname
*/
func deleteNSRecursiveChildren(val string, db *levigo.DB) bool {
	if val == "" {
		return true
	}
	status := true
	children := strings.Split(val, ",")
	for _, child_key := range children {
		child_val := "val::" + strings.Split(child_key, "key::")[1]
		status = status && deleteNSRecursiveChildren(abkleveldb.GetVal(child_key, db), db)
		status = status && abkleveldb.DelKey(child_key, db)
		status = status && abkleveldb.DelKey(child_val, db)
	}
	return status
}

/*
Standard function to delete a namespace with all children below and unroot it.
*/
func DeleteNSRecursive(key string, db *levigo.DB) bool {
	defer UnrootNS(key, db)
	keyname := "key::" + key
	valname := "val::" + key
	keyname_val := abkleveldb.GetVal(keyname, db)
	if abkleveldb.DelKey(keyname, db) {
		if abkleveldb.DelKey(valname, db) {

			if keyname_val == "" {
				return true
			}
			return deleteNSRecursiveChildren(keyname_val, db)
		}
	}
	return false
}
