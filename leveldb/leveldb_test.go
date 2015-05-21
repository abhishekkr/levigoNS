package abkleveldb

import (
	"fmt"
	"os"
	"testing"

	"github.com/jmhodges/levigo"
)

func TestCreateDB(t *testing.T) {
	dbpath := "/tmp/delete-this-leveldb"
	db := CreateDB(dbpath)
	if _, err := os.Stat(dbpath); err != nil {
		t.Error("Fail: CreateDB ain't working.")
	}
	db.Close()
	if os.RemoveAll(dbpath) != nil {
		panic("Fail: Temporary DB files are still present at: " + dbpath)
	}
}

func TestCloseAndDeleteDB(t *testing.T) {
	dbpath := "/tmp/delete-this-leveldb"
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(1 << 10))
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open(dbpath, opts)
	if err != nil {
		t.Error(fmt.Sprintf("Fail: (CloseAndDeleteDB) DB %s Creation failed. %q", dbpath, err))
	}
	CloseAndDeleteDB(dbpath, db)

	if _, err := os.Stat(dbpath); err == nil {
		t.Error("Fail: CreateDB ain't working.")
	}
}

func Test_PushKeyVal(t *testing.T) {
	_key, expectedVal := "name", "levigoNS"
	dbpath := "/tmp/delete-this-leveldb"
	db := CreateDB(dbpath)

	PushKeyVal(_key, expectedVal, db)

	reader := levigo.NewReadOptions()
	defer reader.Close()

	resultVal, err := db.Get(reader, []byte(_key))

	if err != nil {
		t.Error("Fail: (PushKeyVal) Reading key " + _key + " failed")
	}
	if string(resultVal) != expectedVal {
		t.Error("Fail: PushKeyVal sets " + expectedVal + " & gets " + string(resultVal))
	}
	CloseAndDeleteDB(dbpath, db)
}

func Test_GetValues(t *testing.T) {
	_key, expectedVal := "name", "levigoNS"
	dbpath := "/tmp/delete-this-leveldb"
	db := CreateDB(dbpath)

	writer := levigo.NewWriteOptions()
	defer writer.Close()

	keyname := []byte(_key)
	value := []byte(expectedVal)
	err := db.Put(writer, keyname, value)
	if err != nil {
		t.Error("Fail: (GetVal) Pushing key " + _key + " for value " + expectedVal + " failed")
	}

	resultVal := GetVal(_key, db)

	if resultVal != expectedVal {
		t.Error("Fail: GetVal gets " + string(resultVal) + " for set value " + expectedVal)
	}

	CloseAndDeleteDB(dbpath, db)
}

func Test_DelKey(t *testing.T) {
	_key, _val, expectedVal := "name", "levigoNS", ""
	dbpath := "/tmp/delete-this-leveldb"
	db := CreateDB(dbpath)

	writer := levigo.NewWriteOptions()
	defer writer.Close()

	keyname := []byte(_key)
	value := []byte(_val)
	err := db.Put(writer, keyname, value)
	if err != nil {
		t.Error("Fail: (DelKey) Pushing key " + _key + " for value " + _val + " failed")
	}

	statusDelete := DelKey(_key, db)

	reader := levigo.NewReadOptions()
	defer reader.Close()

	resultVal, err := db.Get(reader, []byte(_key))
	if err != nil {
		t.Error("Fail: (DelKey) Reading key " + _key + " failed")
	}
	if string(resultVal) != expectedVal {
		t.Error("Fail: DelKey sets " + string(resultVal))
	}
	if !statusDelete {
		t.Error("Fail: DelKey returns False status")
	}

	CloseAndDeleteDB(dbpath, db)
}
