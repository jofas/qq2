package main

import (
  "fmt"
  "strconv"
  "encoding/json"
  "io"
  "os"
  "net/http"
)
import "github.com/go-redis/redis"
import "github.com/gorilla/mux"

// global reference to the redis client. The client is
// threadsafe
var client = redis.NewClient(&redis.Options{
  Addr     : "localhost:6379",
  Password : "",
  DB       : 0,
})

type Id struct {}

func (i Id) Get(
  w http.ResponseWriter, r *http.Request,
) {
  id, err := strconv.Atoi(mux.Vars(r)["id"])
  if err != nil {
    io.WriteString(w, "-1")
    return
  }


  fmt.Println(id)

  io.WriteString(w, "Hello, id GET!")
}

func (i Id) Patch(
  w http.ResponseWriter, r *http.Request,
){
  id, err := strconv.Atoi(mux.Vars(r)["id"])
  if err != nil {
    io.WriteString(w, "-1")
    return
  }
  fmt.Println(id)

  io.WriteString(w, "Hello, id Patch!")
}

func (i Id) Delete(
  w http.ResponseWriter, r *http.Request,
) {

  id, err := strconv.Atoi(mux.Vars(r)["id"])
  if err != nil {
    io.WriteString(w, "-1")
    return
  }
  fmt.Println(id)

  io.WriteString(w, "Hello, id Delete!")
}


type Students struct {
  id Id
}

// gib alle studenten aus
func (s Students) Get(
  w http.ResponseWriter, r *http.Request,
) {
  keys, err := client.Keys("*").Result()

  if err != nil {
    io.WriteString(w, "-1")
    return
  }

  students := map[string]map[string]interface{}{}

  for key := range keys {
    val, err := client.HGetAll(strconv.Itoa(key)).Result()

    if err != nil {
      io.WriteString(w, "-1")
      return
    }

    n_val := map[string]interface{}{}

    for k, v := range val {
      if k == "Matrikelnummer" || k == "Semester" {
        vi, err := strconv.Atoi(v)

        if err != nil {
          io.WriteString(w, "-1")
          return
        }

        n_val[k] = vi
      } else {
        n_val[k] = v
      }
    }

    students[strconv.Itoa(key)] = n_val
  }

  json, err := json.Marshal(students)

  if err != nil {
    io.WriteString(w, "-1")
    return
  }

  io.WriteString(w, string(json))
}

// leg neuen studenten an
func (s Students) Post(
  w http.ResponseWriter, r *http.Request,
){
  io.WriteString(w, "Hello, POST!")
}

func main() {
  // REST API
  students := Students{id:Id{}}

  r := mux.NewRouter()

  r.HandleFunc("/students", students.Get).
    Methods("GET")
  r.HandleFunc("/students/", students.Get).
    Methods("GET")

  r.HandleFunc("/students", students.Post).
    Methods("POST")
  r.HandleFunc("/students/", students.Post).
    Methods("POST")


  r.HandleFunc("/students/{id}", students.id.Get).
    Methods("GET")
  r.HandleFunc("/students/{id}/", students.id.Get).
    Methods("GET")

  r.HandleFunc("/students/{id}", students.id.Patch).
    Methods("PATCH")
  r.HandleFunc("/students/{id}/", students.id.Patch).
    Methods("PATCH")

  r.HandleFunc("/students/{id}", students.id.Delete).
    Methods("DELETE")
  r.HandleFunc("/students/{id}/", students.id.Delete).
    Methods("DELETE")

  // serve API with the Webserver from stdlib
  http.Handle("/", r)
  http.ListenAndServe("0.0.0.0:8000", nil)
}

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
