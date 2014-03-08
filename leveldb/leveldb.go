package abkleveldb

import (
  "github.com/jmhodges/levigo"

  golerror "github.com/abhishekkr/gol/golerror"
)


/*
Creates a db at provided pathname.
*/
func CreateDB(dbname string) (*levigo.DB) {
  opts := levigo.NewOptions()
  opts.SetCache(levigo.NewLRUCache(1<<10))
  opts.SetCreateIfMissing(true)
  db, err := levigo.Open(dbname, opts)
  if err != nil { golerror.Boohoo("DB " + dbname + " Creation failed.", true) }
  return db
}

/*
Push KeyVal in provided DB handle.
*/
func PushKeyVal(key string, val string, db *levigo.DB) bool{
  writer := levigo.NewWriteOptions()
  defer writer.Close()

  keyname := []byte(key)
  value := []byte(val)
  err := db.Put(writer, keyname, value)
  if err != nil {
    golerror.Boohoo("Key " + key + " insertion failed. It's value was " + val, false)
    return false
  }
  return true
}

/*
Get Value of Key from provided db handle.
*/
func GetVal(key string, db *levigo.DB) string {
  reader := levigo.NewReadOptions()
  defer reader.Close()

  data, err := db.Get(reader, []byte(key))
  if err != nil {
    golerror.Boohoo("Key " + key + " query failed.", false)
    return ""
  }
  return string(data)
}

/*
Del Key from provided DB handle.
*/
func DelKey(key string, db *levigo.DB) bool {
  writer := levigo.NewWriteOptions()
  defer writer.Close()

  err := db.Delete(writer, []byte(key))
  if err != nil {
    golerror.Boohoo("Key " + key + " query failed.", false)
    return false
  }
  return true
}
