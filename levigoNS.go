package abklevigoNS

import (
	"fmt"
	"strings"

	"github.com/jmhodges/levigo"

	golhashmap "github.com/abhishekkr/gol/golhashmap"
	"github.com/abhishekkr/levigoNS/leveldb"
)

/* NameSpaceSeparator could be modified if something other than colon-char ":"
is to be used as separator symbol for NameSpace. */
var (
	NamespaceSeparator = ":"
)

/*
ReadNS reads all direct child values in a given NameSpace
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
		childKey := "val::" + strings.Split(child, "key::")[1]
		childVal := abkleveldb.GetVal(childKey, db)
		if childVal != "" {
			hmap[child] = childVal
		}
	}
	return hmap
}

/*
ReadNSRecursive reads all values belonging to tree below given NameSpace
For e.g.:
  given keys a, a:b, a:b:1, a:b:2, a:b:2:3
  reads for a:b:1, a:b:2, a:b:2:3 if queried for a:b
*/
func ReadNSRecursive(key string, db *levigo.DB) golhashmap.HashMap {
	var hmap golhashmap.HashMap
	hmap = make(golhashmap.HashMap)

	keyname := "key::" + key
	valname := "val::" + key
	keynameVal := abkleveldb.GetVal(keyname, db)
	valnameVal := abkleveldb.GetVal(valname, db)
	if valnameVal != "" {
		hmap[key] = valnameVal
	}
	if keynameVal == "" {
		return hmap
	}
	children := strings.Split(keynameVal, ",")

	for _, childValAsKey := range children {
		childKey := strings.Split(childValAsKey, "key::")[1]
		inhmap := ReadNSRecursive(childKey, db)
		for inHmapKey, inHmapVal := range inhmap {
			hmap[inHmapKey] = inHmapVal
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
appendKey updates the group-val for child keynames
of a parent keyname as required
given a parent keyname and child keyname.
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
CreateNS updates entry with its trail of namespace
given a keyname.
*/
func CreateNS(key string, db *levigo.DB) bool {
	splitIndex := strings.LastIndexAny(key, NamespaceSeparator)
	if splitIndex >= 0 {
		parentKey := key[0:splitIndex]
		childKey := key[splitIndex+1:]

		if appendKey(parentKey, childKey, db) {
			return CreateNS(parentKey, db)
		}
		return false
	}
	return true
}

/*
PushNS feeds in namespace entries given namespace key and val.
*/
func PushNS(key string, val string, db *levigo.DB) bool {
	CreateNS(key, db)
	key = "val::" + key
	return abkleveldb.PushKeyVal(key, val, db)
}

/*
UnrootNS update key's presence from it's parent's  group-val of child key names.
*/
func UnrootNS(key string, db *levigo.DB) bool {
	statusParentUnroot, statusParentUpdate := true, true
	splitIndex := strings.LastIndexAny(key, NamespaceSeparator)
	if splitIndex < 0 {
		return true
	}
	parentKey := key[0:splitIndex]
	selfKeyname := fmt.Sprintf("key::%s", key)
	parentKeyname := fmt.Sprintf("key::%s", parentKey)
	parentKeynameVal := abkleveldb.GetVal(parentKeyname, db)
	if parentKeynameVal == "" {
		return true
	}
	parentKeynameValElem := strings.Split(parentKeynameVal, ",")

	_tmpArray := make([]string, len(parentKeynameValElem))
	_tmpArrayIdx := 0
	for _, elem := range parentKeynameValElem {
		if elem != selfKeyname {
			if elem == "" {
				continue
			}
			_tmpArray[_tmpArrayIdx] = elem
			_tmpArrayIdx++
		}
	}

	if _tmpArrayIdx > 1 {
		parentKeynameVal = strings.Join(_tmpArray[0:_tmpArrayIdx-1], ",")
	} else {
		parentKeynameVal = _tmpArray[0]
	}

	if parentKeynameVal == "" {
		statusParentUnroot = UnrootNS(parentKey, db)
	}

	statusParentUpdate = abkleveldb.PushKeyVal(parentKeyname, parentKeynameVal, db)

	return statusParentUnroot && statusParentUpdate
}

/*
DeleteNSKey directly deletes a child key-val and unroot it from parent.
*/
func DeleteNSKey(key string, db *levigo.DB) bool {
	defer UnrootNS(key, db)
	selfVal := "val::" + key
	if abkleveldb.DelKey(selfVal, db) {
		keyname := "key::" + key
		if abkleveldb.DelKey(keyname, db) {
			return true
		}
	}
	return false
}

/*
deleteNSChildren deletes direct children of any keyname.
*/
func deleteNSChildren(val string, db *levigo.DB) bool {
	status := true
	children := strings.Split(val, ",")
	for _, childKey := range children {
		childVal := "val::" + strings.Split(childKey, "key::")[1]
		status = status && abkleveldb.DelKey(childKey, db)
		status = status && abkleveldb.DelKey(childVal, db)
	}
	return status
}

/*
DeleteNS deletes a namespace with all direct children and unroot it.
*/
func DeleteNS(key string, db *levigo.DB) bool {
	defer UnrootNS(key, db)
	selfVal := "val::" + key
	if abkleveldb.DelKey(selfVal, db) {
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
deleteNSRecursiveChildren deletes recursive children of any keyname.
*/
func deleteNSRecursiveChildren(val string, db *levigo.DB) bool {
	if val == "" {
		return true
	}
	status := true
	children := strings.Split(val, ",")
	for _, childKey := range children {
		childVal := "val::" + strings.Split(childKey, "key::")[1]
		status = status && deleteNSRecursiveChildren(abkleveldb.GetVal(childKey, db), db)
		status = status && abkleveldb.DelKey(childKey, db)
		status = status && abkleveldb.DelKey(childVal, db)
	}
	return status
}

/*
DeleteNSRecursive to delete a namespace with all children below and unroot it.
*/
func DeleteNSRecursive(key string, db *levigo.DB) bool {
	defer UnrootNS(key, db)
	keyname := "key::" + key
	valname := "val::" + key
	keynameVal := abkleveldb.GetVal(keyname, db)
	if abkleveldb.DelKey(keyname, db) {
		if abkleveldb.DelKey(valname, db) {

			if keynameVal == "" {
				return true
			}
			return deleteNSRecursiveChildren(keynameVal, db)
		}
	}
	return false
}
