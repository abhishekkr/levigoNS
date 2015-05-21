package abklevigoNS

import (
	"fmt"
	"strings"
	"testing"

	"github.com/jmhodges/levigo"

	golassert "github.com/abhishekkr/gol/golassert"
	golhashmap "github.com/abhishekkr/gol/golhashmap"
	abkleveldb "github.com/abhishekkr/levigoNS/leveldb"
)

var (
	dbpath = "/tmp/delete-this-levigoNS"
)

func setupTestData(db *levigo.DB) {
	abkleveldb.PushKeyVal("key::abc", "key::abc:name", db)
	abkleveldb.PushKeyVal("val::abc:name", "ABC XYZ", db)
	abkleveldb.PushKeyVal("key::abc:name", "key::abc:name:first,key::abc:name:last", db)
	abkleveldb.PushKeyVal("val::abc:name:first", "ABC", db)
	abkleveldb.PushKeyVal("val::abc:name:last", "XYZ", db)
}

func TestReadNS(t *testing.T) {
	_parentKey, _key := "abc", "abc:name"
	expectedParentKeyVal := "key::abc:name,ABC XYZ"
	expectedKeyVal := "key::abc:name:first,ABC\nkey::abc:name:last,XYZ"

	db := abkleveldb.CreateDB(dbpath)
	setupTestData(db)

	fmt.Printf("%q", ReadNS(_parentKey, db))
	resultParentKeyVal := golhashmap.HashMapToCSV(ReadNS(_parentKey, db))
	resultKeyVal := golhashmap.HashMapToCSV(ReadNS(_key, db))

	golassert.AssertEqualStringArray(
		strings.Split(expectedParentKeyVal, "\n"),
		strings.Split(resultParentKeyVal, "\n"),
	)
	golassert.AssertEqualStringArray(
		strings.Split(expectedKeyVal, "\n"),
		strings.Split(resultKeyVal, "\n"),
	)

	abkleveldb.CloseAndDeleteDB(dbpath, db)
}

func TestReadNSRecursive(t *testing.T) {
	_parentKey, _key, _childKey := "abc", "abc:name", "abc:name:last"
	expectedParentKeyVal := "abc:name,ABC XYZ\nabc:name:first,ABC\nabc:name:last,XYZ"
	expectedKeyVal := "abc:name,ABC XYZ\nabc:name:first,ABC\nabc:name:last,XYZ"
	expectedChildKeyVal := "abc:name:last,XYZ"

	db := abkleveldb.CreateDB(dbpath)
	setupTestData(db)

	resultParentKeyVal := golhashmap.HashMapToCSV(ReadNSRecursive(_parentKey, db))
	resultKeyVal := golhashmap.HashMapToCSV(ReadNSRecursive(_key, db))
	resultChildKeyVal := golhashmap.HashMapToCSV(ReadNSRecursive(_childKey, db))

	golassert.AssertEqualStringArray(
		strings.Split(expectedParentKeyVal, "\n"),
		strings.Split(resultParentKeyVal, "\n"),
	)
	golassert.AssertEqualStringArray(
		strings.Split(expectedKeyVal, "\n"),
		strings.Split(resultKeyVal, "\n"),
	)
	golassert.AssertEqualStringArray(
		strings.Split(expectedChildKeyVal, "\n"),
		strings.Split(resultChildKeyVal, "\n"),
	)

	abkleveldb.CloseAndDeleteDB(dbpath, db)
}

func TestIfChildExists(t *testing.T) {
	_parentKeyValue := "key::abc:name"
	_parentKeyChild := "key::abc:name"
	_keyValue := "key::abc:name:first,key::abc:name:last"
	_keyChild := "key::abc:name:first"

	if ifChildExists(_parentKeyChild, _parentKeyValue) != true {
		t.Error(fmt.Sprintf("Fail: ifChildExists thinks %s is not child-key of %s", _parentKeyChild, _parentKeyValue))
	}
	if ifChildExists(_keyChild, _keyValue) != true {
		t.Error(fmt.Sprintf("Fail: ifChildExists thinks %s is not child-key of %s", _keyChild, _keyValue))
	}
	if ifChildExists(_parentKeyChild, _keyValue) != false {
		t.Error(fmt.Sprintf("Fail: ifChildExists thinks %s is child-key of %s", _parentKeyChild, _keyValue))
	}
}

func TestAppendKey(t *testing.T) {
	db := abkleveldb.CreateDB(dbpath)

	status := appendKey("abc:name:first", "title", db)
	expectedVal := "key::abc:name:first:title"
	resultVal := abkleveldb.GetVal("key::abc:name:first", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}
	if !status {
		t.Error("Fail: Failed Status for", expectedVal)
	}

	status = appendKey("abc", "name", db)
	expectedVal = "key::abc:name"
	resultVal = abkleveldb.GetVal("key::abc", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}
	if !status {
		t.Error("Fail: Failed Status for", expectedVal)
	}

	status = appendKey("abc", "age", db)
	expectedVal = "key::abc:name,key::abc:age"
	resultVal = abkleveldb.GetVal("key::abc", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}
	if !status {
		t.Error("Fail: Failed Status for", expectedVal)
	}

	abkleveldb.CloseAndDeleteDB(dbpath, db)
}

func TestCreateNS(t *testing.T) {
	db := abkleveldb.CreateDB(dbpath)

	status := CreateNS("abc:name:first", db)
	expectedVal := "key::abc:name:first"
	resultVal := abkleveldb.GetVal("key::abc:name", db)
	if expectedVal != resultVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}
	if !status {
		t.Error("Fail: Failed Status for", expectedVal)
	}

	status = CreateNS("abc:name:last", db)
	expectedVal = "key::abc:name:first,key::abc:name:last"
	resultVal = abkleveldb.GetVal("key::abc:name", db)
	if expectedVal != resultVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}
	if !status {
		t.Error("Fail: Failed Status for", expectedVal)
	}

	status = CreateNS("abc:name:last", db)
	expectedVal = "key::abc:name:first,key::abc:name:last"
	resultVal = abkleveldb.GetVal("key::abc:name", db)
	if expectedVal != resultVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}
	if !status {
		t.Error("Fail: Failed Status for", expectedVal)
	}

	abkleveldb.CloseAndDeleteDB(dbpath, db)
}

func TestPushNS(t *testing.T) {
	db := abkleveldb.CreateDB(dbpath)

	status := PushNS("abc:name", "ABC XYZ", db)
	expectedVal := "ABC XYZ"
	resultVal := abkleveldb.GetVal("val::abc:name", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}
	if !status {
		t.Error("Fail: Failed Status for", expectedVal)
	}

	expectedVal = "key::abc:name"
	resultVal = abkleveldb.GetVal("key::abc", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}

	abkleveldb.CloseAndDeleteDB(dbpath, db)
}

func TestUnrootNS(t *testing.T) {
	db := abkleveldb.CreateDB(dbpath)
	setupTestData(db)

	status := UnrootNS("abc:name:first", db)
	expectedVal := "key::abc:name:last"
	resultVal := abkleveldb.GetVal("key::abc:name", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}
	if !status {
		t.Error("Fail: Failed Status for", expectedVal)
	}

	status = UnrootNS("abc:name:first", db)
	expectedVal = "key::abc:name:last"
	resultVal = abkleveldb.GetVal("key::abc:name", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}
	if !status {
		t.Error("Fail: Failed Status for", expectedVal)
	}

	status = UnrootNS("abc:name:none", db)
	expectedVal = "key::abc:name:last"
	resultVal = abkleveldb.GetVal("key::abc:name", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}
	if !status {
		t.Error("Fail: Failed Status for", expectedVal)
	}

	status = UnrootNS("abc:name:last", db)
	expectedVal = ""
	resultVal = abkleveldb.GetVal("key::abc:name", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}
	if !status {
		t.Error("Fail: Failed Status for", expectedVal)
	}

	expectedVal = ""
	resultVal = abkleveldb.GetVal("key::abc", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}
	if !status {
		t.Error("Fail: Failed Status for", expectedVal)
	}

	abkleveldb.CloseAndDeleteDB(dbpath, db)
}

func TestDeleteNSKey(t *testing.T) {
	db := abkleveldb.CreateDB(dbpath)
	setupTestData(db)

	status := DeleteNSKey("abc:name:last", db)

	expectedVal := ""
	resultVal := abkleveldb.GetVal("val::abc:name:last", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}
	if !status {
		t.Error("Fail: Failed Status for", expectedVal)
	}

	expectedVal = "ABC"
	resultVal = abkleveldb.GetVal("val::abc:name:first", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}

	status = DeleteNSKey("abc:name:last", db)
	if !status {
		t.Error("Fail: Success in deleting non-existent key.")
	}

	abkleveldb.CloseAndDeleteDB(dbpath, db)
}

func TestDeleteNSChildren(t *testing.T) {
	db := abkleveldb.CreateDB(dbpath)
	setupTestData(db)

	expectedVal := "ABC XYZ"
	resultVal := abkleveldb.GetVal("val::abc:name", db)
	if resultVal != expectedVal {
		t.Error("Fail: Pre-req is bad.")
	}

	status := deleteNSChildren("key::abc:name", db)
	expectedVal = ""
	resultVal = abkleveldb.GetVal("val::abc:name", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}
	if !status {
		t.Error("Fail: Failed Status for", expectedVal)
	}

	expectedVal = ""
	resultVal = abkleveldb.GetVal("key::abc:name", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}

	expectedVal = "XYZ"
	resultVal = abkleveldb.GetVal("val::abc:name:last", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}

	status = deleteNSChildren("key::abc:name:first,key::abc:name:last", db)
	expectedVal = ""
	resultVal = abkleveldb.GetVal("val::abc:name:first", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}
	if !status {
		t.Error("Fail: Failed Status for", expectedVal)
	}

	abkleveldb.CloseAndDeleteDB(dbpath, db)
}

func TestDeleteNS(t *testing.T) {
	db := abkleveldb.CreateDB(dbpath)
	setupTestData(db)

	status := DeleteNS("abc", db)
	expectedVal := ""
	resultVal := abkleveldb.GetVal("val::abc", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}
	if !status {
		t.Error("Fail: Failed Status for", expectedVal)
	}

	expectedVal = ""
	resultVal = abkleveldb.GetVal("val::abc:name", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}

	expectedVal = "XYZ"
	resultVal = abkleveldb.GetVal("val::abc:name:last", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}

	abkleveldb.CloseAndDeleteDB(dbpath, db)
}

func TestDeleteNSRecursiveChildren(t *testing.T) {
	db := abkleveldb.CreateDB(dbpath)
	setupTestData(db)

	status := deleteNSRecursiveChildren("key::abc:name:first", db)
	expectedVal := ""
	resultVal := abkleveldb.GetVal("val::abc:name:first", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}
	if !status {
		t.Error("Fail: Failed Status for", expectedVal)
	}

	status = deleteNSRecursiveChildren("key::abc", db)
	expectedVal = ""
	resultVal = abkleveldb.GetVal("key::abc", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}
	if !status {
		t.Error("Fail: Failed Status for", expectedVal)
	}

	expectedVal = ""
	resultVal = abkleveldb.GetVal("key::abc:name", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}

	expectedVal = ""
	resultVal = abkleveldb.GetVal("val::abc:name:last", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}

	abkleveldb.CloseAndDeleteDB(dbpath, db)
}

func TestDeleteNSRecursive(t *testing.T) {
	db := abkleveldb.CreateDB(dbpath)
	setupTestData(db)

	status := DeleteNSRecursive("abc", db)

	expectedVal := ""
	resultVal := abkleveldb.GetVal("key::abc", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}
	if !status {
		t.Error("Fail: Failed Status for", expectedVal)
	}

	expectedVal = ""
	resultVal = abkleveldb.GetVal("key::abc:name", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}

	expectedVal = ""
	resultVal = abkleveldb.GetVal("val::abc:name:first", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}

	expectedVal = ""
	resultVal = abkleveldb.GetVal("val::abc:name:last", db)
	if resultVal != expectedVal {
		t.Error("Fail: Get", resultVal, "instead of", expectedVal)
	}

	abkleveldb.CloseAndDeleteDB(dbpath, db)
}
