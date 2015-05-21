package abkleveldb

import (
	"fmt"
	"os"

	"github.com/jmhodges/levigo"

	golerror "github.com/abhishekkr/gol/golerror"
)

/*
CreateDB creates a db at provided dbpath.
*/
func CreateDB(dbpath string) *levigo.DB {
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(1 << 10))
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open(dbpath, opts)
	if err != nil {
		errMsg := fmt.Sprintf("DB %s Creation failed. %q", dbpath, err)
		golerror.Boohoo(errMsg, true)
	}
	return db
}

/*
CloseAndDeleteDB closes and deletes a db given handle and dbpath.
Useful in use and throw implementations. And also tests.
*/
func CloseAndDeleteDB(dbpath string, db *levigo.DB) {
	db.Close()
	if os.RemoveAll(dbpath) != nil {
		panic("Fail: Temporary DB files are still present at: " + dbpath)
	}
}

/*
PushKeyVal push KeyVal in provided DB handle.
*/
func PushKeyVal(key string, val string, db *levigo.DB) bool {
	writer := levigo.NewWriteOptions()
	defer writer.Close()

	keyname := []byte(key)
	value := []byte(val)
	err := db.Put(writer, keyname, value)
	if err != nil {
		golerror.Boohoo("Key "+key+" insertion failed. It's value was "+val, false)
		return false
	}
	return true
}

/*
GetVal gets value of Key from provided db handle.
*/
func GetVal(key string, db *levigo.DB) string {
	reader := levigo.NewReadOptions()
	defer reader.Close()

	data, err := db.Get(reader, []byte(key))
	if err != nil {
		golerror.Boohoo("Key "+key+" query failed.", false)
		return ""
	}
	return string(data)
}

/*
DelKey deletes key from provided DB handle.
*/
func DelKey(key string, db *levigo.DB) bool {
	writer := levigo.NewWriteOptions()
	defer writer.Close()

	err := db.Delete(writer, []byte(key))
	if err != nil {
		golerror.Boohoo("Key "+key+" query failed.", false)
		return false
	}
	return true
}
