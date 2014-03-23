package abklevigoNS

import (
  "fmt"
  "testing"

  "github.com/jmhodges/levigo"

  abkleveldb "github.com/abhishekkr/levigoNS/leveldb"
  golhashmap "github.com/abhishekkr/gol/golhashmap"
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
  _parent_key, _key := "abc", "abc:name"
  expected_parent_key_val := "key::abc:name,ABC XYZ\n"
  expected_key_val := "key::abc:name:first,ABC\nkey::abc:name:last,XYZ\n"

  db := abkleveldb.CreateDB(dbpath)
  setupTestData(db)

  result_parent_key_val := golhashmap.Hashmap_to_csv(ReadNS(_parent_key, db))
  result_key_val := golhashmap.Hashmap_to_csv(ReadNS(_key, db))

  if result_parent_key_val != expected_parent_key_val {
    t.Error( fmt.Sprintf("Fail: ReadNS failed for getting descendents. Result: %q, Expected: %q.", result_parent_key_val, expected_parent_key_val) )
  }
  if result_key_val != expected_key_val {
    t.Error( fmt.Sprintf("Fail: ReadNS failed for getting descendents. Result: %q, Expected: %q.", result_key_val, expected_key_val) )
  }

  abkleveldb.CloseAndDeleteDB(dbpath, db)
}


func TestReadNSRecursive(t *testing.T) {
  _parent_key, _key, _child_key := "abc", "abc:name", "abc:name:last"
  expected_parent_key_val := "abc:name,ABC XYZ\nabc:name:first,ABC\nabc:name:last,XYZ\n"
  expected_key_val := "abc:name,ABC XYZ\nabc:name:first,ABC\nabc:name:last,XYZ\n"
  expected_child_key_val := "abc:name:last,XYZ\n"

  db := abkleveldb.CreateDB(dbpath)
  setupTestData(db)

  result_parent_key_val := golhashmap.Hashmap_to_csv(ReadNSRecursive(_parent_key, db))
  result_key_val := golhashmap.Hashmap_to_csv(ReadNSRecursive(_key, db))
  result_child_key_val := golhashmap.Hashmap_to_csv(ReadNSRecursive(_child_key, db))

  if result_parent_key_val != expected_parent_key_val {
    t.Error( fmt.Sprintf("Fail: ReadNS failed for getting descendents. Result: %q, Expected: %q.", result_parent_key_val, expected_parent_key_val) )
  }
  if result_key_val != expected_key_val {
    t.Error( fmt.Sprintf("Fail: ReadNS failed for getting descendents. Result: %q, Expected: %q.", result_key_val, expected_key_val) )
  }
  if result_child_key_val != expected_child_key_val {
    t.Error( fmt.Sprintf("Fail: ReadNS failed for getting descendents. Result: %q, Expected: %q.", result_child_key_val, expected_child_key_val) )
  }

  abkleveldb.CloseAndDeleteDB(dbpath, db)
}


func TestIfChildExists(t *testing.T) {
  _parent_key_value := "key::abc:name"
  _parent_key_child := "key::abc:name"
  _key_value := "key::abc:name:first,key::abc:name:last"
  _key_child := "key::abc:name:first"

  if ifChildExists(_parent_key_child, _parent_key_value) != true {
    t.Error(fmt.Sprintf("Fail: ifChildExists thinks %s is not child-key of %s", _parent_key_child, _parent_key_value))
  }
  if ifChildExists(_key_child, _key_value) != true {
    t.Error(fmt.Sprintf("Fail: ifChildExists thinks %s is not child-key of %s", _key_child, _key_value))
  }
  if ifChildExists(_parent_key_child, _key_value) != false {
    t.Error(fmt.Sprintf("Fail: ifChildExists thinks %s is child-key of %s", _parent_key_child, _key_value))
  }
}


func TestAppendKey(t *testing.T) {
  db := abkleveldb.CreateDB(dbpath)

  status := appendKey("abc:name:first", "title", db)
  expected_val := "key::abc:name:first:title"
  result_val := abkleveldb.GetVal("key::abc:name:first", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }
  if ! status { t.Error("Fail: Failed Status for", expected_val) }

  status = appendKey("abc", "name", db)
  expected_val = "key::abc:name"
  result_val = abkleveldb.GetVal("key::abc", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }
  if ! status { t.Error("Fail: Failed Status for", expected_val) }

  status = appendKey("abc", "age", db)
  expected_val = "key::abc:name,key::abc:age"
  result_val = abkleveldb.GetVal("key::abc", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }
  if ! status { t.Error("Fail: Failed Status for", expected_val) }

  abkleveldb.CloseAndDeleteDB(dbpath, db)
}


func TestCreateNS(t *testing.T) {
  db := abkleveldb.CreateDB(dbpath)

  status := CreateNS("abc:name:first", db)
  expected_val := "key::abc:name:first"
  result_val := abkleveldb.GetVal("key::abc:name", db)
  if expected_val != result_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }
  if ! status { t.Error("Fail: Failed Status for", expected_val) }

  status = CreateNS("abc:name:last", db)
  expected_val = "key::abc:name:first,key::abc:name:last"
  result_val = abkleveldb.GetVal("key::abc:name", db)
  if expected_val != result_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }
  if ! status { t.Error("Fail: Failed Status for", expected_val) }

  status = CreateNS("abc:name:last", db)
  expected_val = "key::abc:name:first,key::abc:name:last"
  result_val = abkleveldb.GetVal("key::abc:name", db)
  if expected_val != result_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }
  if ! status { t.Error("Fail: Failed Status for", expected_val) }

  abkleveldb.CloseAndDeleteDB(dbpath, db)
}


func TestPushNS(t *testing.T) {
  db := abkleveldb.CreateDB(dbpath)

  status := PushNS("abc:name", "ABC XYZ", db)
  expected_val := "ABC XYZ"
  result_val := abkleveldb.GetVal("val::abc:name", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }
  if ! status { t.Error("Fail: Failed Status for", expected_val) }

  expected_val = "key::abc:name"
  result_val = abkleveldb.GetVal("key::abc", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }

  abkleveldb.CloseAndDeleteDB(dbpath, db)
}


func TestUnrootNS(t *testing.T) {
  db := abkleveldb.CreateDB(dbpath)
  setupTestData(db)

  status := UnrootNS("abc:name:first", db)
  expected_val := "key::abc:name:last"
  result_val := abkleveldb.GetVal("key::abc:name", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }
  if ! status { t.Error("Fail: Failed Status for", expected_val) }

  status = UnrootNS("abc:name:first", db)
  expected_val = "key::abc:name:last"
  result_val = abkleveldb.GetVal("key::abc:name", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }
  if ! status { t.Error("Fail: Failed Status for", expected_val) }

  status = UnrootNS("abc:name:none", db)
  expected_val = "key::abc:name:last"
  result_val = abkleveldb.GetVal("key::abc:name", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }
  if ! status { t.Error("Fail: Failed Status for", expected_val) }

  status = UnrootNS("abc:name:last", db)
  expected_val = ""
  result_val = abkleveldb.GetVal("key::abc:name", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }
  if ! status { t.Error("Fail: Failed Status for", expected_val) }

  expected_val = ""
  result_val = abkleveldb.GetVal("key::abc", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }
  if ! status { t.Error("Fail: Failed Status for", expected_val) }

  abkleveldb.CloseAndDeleteDB(dbpath, db)
}


func TestDeleteNSKey(t *testing.T) {
  db := abkleveldb.CreateDB(dbpath)
  setupTestData(db)

  status := DeleteNSKey("abc:name:last", db)

  expected_val := ""
  result_val := abkleveldb.GetVal("val::abc:name:last", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }
  if ! status { t.Error("Fail: Failed Status for", expected_val) }

  expected_val = "ABC"
  result_val = abkleveldb.GetVal("val::abc:name:first", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }

  status = DeleteNSKey("abc:name:last", db)
  if ! status { t.Error("Fail: Success in deleting non-existent key.") }

  abkleveldb.CloseAndDeleteDB(dbpath, db)
}


func TestDeleteNSChildren(t *testing.T) {
  db := abkleveldb.CreateDB(dbpath)
  setupTestData(db)

  expected_val := "ABC XYZ"
  result_val := abkleveldb.GetVal("val::abc:name", db)
  if result_val != expected_val {
    t.Error("Fail: Pre-req is bad.")
  }

  status := deleteNSChildren("key::abc:name", db)
  expected_val = ""
  result_val = abkleveldb.GetVal("val::abc:name", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }
  if ! status { t.Error("Fail: Failed Status for", expected_val) }

  expected_val = ""
  result_val = abkleveldb.GetVal("key::abc:name", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }

  expected_val = "XYZ"
  result_val = abkleveldb.GetVal("val::abc:name:last", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }

  status = deleteNSChildren("key::abc:name:first,key::abc:name:last", db)
  expected_val = ""
  result_val = abkleveldb.GetVal("val::abc:name:first", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }
  if ! status { t.Error("Fail: Failed Status for", expected_val) }

  abkleveldb.CloseAndDeleteDB(dbpath, db)
}


func TestDeleteNS(t *testing.T) {
  db := abkleveldb.CreateDB(dbpath)
  setupTestData(db)

  status := DeleteNS("abc", db)
  expected_val := ""
  result_val := abkleveldb.GetVal("val::abc", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }
  if ! status { t.Error("Fail: Failed Status for", expected_val) }

  expected_val = ""
  result_val = abkleveldb.GetVal("val::abc:name", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }

  expected_val = "XYZ"
  result_val = abkleveldb.GetVal("val::abc:name:last", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }

  abkleveldb.CloseAndDeleteDB(dbpath, db)
}


func TestDeleteNSRecursiveChildren(t *testing.T) {
  db := abkleveldb.CreateDB(dbpath)
  setupTestData(db)

  status := deleteNSRecursiveChildren("key::abc:name:first", db)
  expected_val := ""
  result_val := abkleveldb.GetVal("val::abc:name:first", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }
  if ! status { t.Error("Fail: Failed Status for", expected_val) }

  status = deleteNSRecursiveChildren("key::abc", db)
  expected_val = ""
  result_val = abkleveldb.GetVal("key::abc", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }
  if ! status { t.Error("Fail: Failed Status for", expected_val) }

  expected_val = ""
  result_val = abkleveldb.GetVal("key::abc:name", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }

  expected_val = ""
  result_val = abkleveldb.GetVal("val::abc:name:last", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }

  abkleveldb.CloseAndDeleteDB(dbpath, db)
}


func TestDeleteNSRecursive(t *testing.T) {
  db := abkleveldb.CreateDB(dbpath)
  setupTestData(db)

  status := DeleteNSRecursive("abc", db)

  expected_val := ""
  result_val := abkleveldb.GetVal("key::abc", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }
  if ! status { t.Error("Fail: Failed Status for", expected_val) }

  expected_val = ""
  result_val = abkleveldb.GetVal("key::abc:name", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }

  expected_val = ""
  result_val = abkleveldb.GetVal("val::abc:name:first", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }

  expected_val = ""
  result_val = abkleveldb.GetVal("val::abc:name:last", db)
  if result_val != expected_val {
    t.Error("Fail: Get", result_val, "instead of", expected_val)
  }

  abkleveldb.CloseAndDeleteDB(dbpath, db)
}

