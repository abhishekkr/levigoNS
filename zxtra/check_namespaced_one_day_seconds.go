package main

import (
  "fmt"
  "runtime"
  "time"
  "flag"

  "github.com/jmhodges/levigo"

  abklevigoNS "github.com/abhishekkr/levigoNS"
  abkleveldb "github.com/abhishekkr/levigoNS/leveldb"
)

var (
  dbpath = flag.String("db", "/tmp/LevigoNS00", "the path to your db")
)

func EverySecondOfHour(hour int, check string, db *levigo.DB) {
  for sec := 0; sec < 3600; sec++{
    nukey := fmt.Sprintf("127.0.0.1:%s:2013:10:26:%d:%d",
                        check, hour, sec)
    if sec%500 != 0 { continue }
    val := "up"
    if sec%1000 == 0 {
      val = "down"
    }
    abklevigoNS.PushNS(nukey, val, db)
  }
  fmt.Printf("Hour %d done. Enter 'yes' anytime to end Push action.\n", hour)
}

func WriteMap(db *levigo.DB){
  for hour := 0; hour < 24; hour++ {
    go EverySecondOfHour(hour, "status", db)
  }
  for {
    for {
      var quit string
      fmt.Scanf("%s", &quit)
      if quit == "y" || quit == "yes" {
        return
      }
    }
    time.Sleep(10 * time.Second)
  }
}

func ReadMap(key string, db *levigo.DB){
  var hmap map[string]string
  hmap = make(map[string]string)
  hmap = abklevigoNS.ReadNS(key, db)
  fmt.Println("Total Child Keys found:", len(hmap))
  for k,v := range hmap {
    fmt.Printf("%s => %s\n", k, v)
  }
}

func PrintMapRecursive(m abklevigoNS.HashMap){
  for k,v := range m {
    fmt.Println("val for key:", k, v)
  }
}

func main(){
  start_time := time.Now()
  runtime.GOMAXPROCS(runtime.NumCPU())
  var db *levigo.DB
  fmt.Println("Your DB is referenced at", *dbpath)
  create_start_time := time.Now()
  db = abkleveldb.CreateDB(*dbpath)
  WriteMap(db)
  fmt.Println("Writing is over.")
  ReadMap("127.0.0.1:status:2013:10:26:12", db)
  result := abklevigoNS.ReadNSRecursive("127.0.0.1:status", db)
  read_start_time := time.Now()
  PrintMapRecursive( result )
  ReadMap("127.0.0.1:status:2013:10:26", db)
  fmt.Println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
  fmt.Printf("\n\nStatistics:\n\tStarted at: %q\n", start_time)
  fmt.Printf("\tCreating DB: %q\n", create_start_time)
  fmt.Printf("\tReading DB: %q\n\tRead For an Hour: %q\n", read_start_time, time.Now())
  fmt.Println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
  fmt.Println(len(result))
}
