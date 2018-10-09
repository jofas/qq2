package main

import (
  "fmt"
  //"strconv"
  "encoding/json"
  "io"
  "os"
)
import "github.com/go-redis/redis"

/* EXAMPLE STUDENT

"0": {
  "Vorname": "Max",
  "Nachname": "Mustermann",
  "Matrikelnummer": 0,
  "Studiengang": "AI",
  "Semester": 1,
  "E-Mail": "m@m.de",
}

*/

// import some example students as json
func import_backup() map[string]map[string]interface{} {
  fi, err := os.Open("students.json")

  if err != nil {
    panic(err)
  }

  defer func() {
    if err := fi.Close(); err != nil {
      panic(err)
    }
  }()

  buf := make([]byte, 1024)

  n, err := fi.Read(buf);
  if err != nil && err != io.EOF {
    panic(err)
  }

  students := map[string]map[string]interface{}{}

  if err := json.Unmarshal(buf[:n], &students);
  err != nil {
    panic(err)
  }

  return students
}

func main() {

  students := import_backup()
  fmt.Println(students)

  client := redis.NewClient(&redis.Options{
    Addr     : "localhost:6379",
    Password : "",
    DB       : 0,
  })


  for key, value := range students {
    err := client.HMSet(key,value).Err()
    if err != nil {
      panic(err)
    }
  }

  keys:= client.Keys("*")

  fmt.Println(keys)

  /*
  val, err := client.HGetAll(key).Result()

  if err != nil {
    panic(err)
  }

  fmt.Println(val)
  */
}
