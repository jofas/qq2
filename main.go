package main

import (
  "fmt"
  "strconv"
  "encoding/json"
  "io"
  "os"
  "net/http"
  //"sync"
)
import "github.com/go-redis/redis"
import "github.com/gorilla/mux"
import "github.com/confluentinc/confluent-kafka-go/kafka"

// global reference to the redis client. The client is
// threadsafe
var client = redis.NewClient(&redis.Options{
  Addr     : "localhost:6379",
  Password : "",
  DB       : 0,
})

// global reference to the producer (for the messaging)
//var producer kafka.Producer
// mutex for using the producer
//var m_producer = &sync.Mutex{}

const msg_template =
  "{service_name:1_GoKit_2,operation:'%s',message:'%s',}"

var topic = "logging"

var producer = initProducer()

func initProducer() kafka.Producer {
  producer, err := kafka.NewProducer(&kafka.ConfigMap{
    "bootstrap.servers": "qq2.ddnss.de:9092",
  })
  if err != nil {
    panic(err)
  }
  return *producer
}

func log(op string, msg string) {
  if err := producer.Produce(&kafka.Message{
    TopicPartition: kafka.TopicPartition{
      Topic: &topic,
      Partition: kafka.PartitionAny,
    },
    Value: []byte(fmt.Sprintf(msg_template, op, msg)),
  }, nil); err != nil {
    fmt.Println(err)
  }
}

type Id struct {}

// return student with {id}
func (i Id) Get(
  w http.ResponseWriter, r *http.Request,
) {
  id := mux.Vars(r)["id"]

  student, err := client.HGetAll(id).Result()

  if err != nil || len(student) == 0 {
    fmt.Println("ERROR: 'HGETALL id' did not work")
    log("GET students/"+id, "404")
    w.WriteHeader(404)
    return
  }

  json, err := json.Marshal(student)

  if err != nil {
    fmt.Println("ERROR: parsing student did not work")
    log("GET students/"+id, "500")
    w.WriteHeader(500)
    return
  }

  io.WriteString(w, string(json))
  log("GET students/"+id, "200")
}

// update student
func (i Id) Patch(
  w http.ResponseWriter, r *http.Request,
){
  id := mux.Vars(r)["id"]

  b := make([]byte, 1024)

  n, err := r.Body.Read(b)

  if err != nil && err != io.EOF {
    fmt.Println("ERROR: Reading HTTP Body did not work")
    log("PATCH students/"+id, "500")
    w.WriteHeader(500)
    return
  }

  u_student := map[string]interface{}{}

  if err := json.Unmarshal(b[:n], &u_student);err != nil {
    fmt.Println("ERROR: JSON parsing did not work")
    log("PATCH students/"+id, "500")
    w.WriteHeader(500)
    return
  }

  if _, err := client.HMSet(id, u_student).Result();
  err != nil {
    fmt.Println("ERROR: HMSET did not work")
    log("PATCH students/"+id, "500")
    w.WriteHeader(500)
    return
  }

  // OK
  w.WriteHeader(200)
  log("PATCH students/"+id, "200")
}

// delete student
func (i Id) Delete(
  w http.ResponseWriter, r *http.Request,
) {
  id := mux.Vars(r)["id"]

  amount, err := client.Del(id).Result()

  if err != nil {
    fmt.Println("ERROR: 'DEL id' did not work")
    log("DELETE students/"+id, "500")
    w.WriteHeader(500)
    return
  }

  // no student was deleted -> student did not exists so
  // 404
  if amount == 0 {
    log("DELETE students/"+id, "404")
    w.WriteHeader(404)
    return
  }

  // OK
  w.WriteHeader(200)
  log("DELETE students/"+id, "200")
}


type Students struct {
  id Id
}

// return all students
func (s Students) Get(
  w http.ResponseWriter, r *http.Request,
) {
  // get all keys safed in redis
  keys, err := client.Keys("*").Result()

  if err != nil {
    fmt.Println("ERROR: 'KEYS *' did not work")
    log("GET students/", "500")
    w.WriteHeader(500)
    return
  }

  // map containing all students at the end
  students := map[string]map[string]string{}

  // iterate redis keys
  for _, key := range keys {
    // we only return students and not our next id variable
    if key == "next" { continue }

    // get the student at key from redis
    val, err := client.HGetAll(key).Result()

    if err != nil {
      fmt.Println("ERROR: 'HGETALL key' did not work")
      log("GET students/", "500")
      w.WriteHeader(500)
      return
    }

    students[key] = val
  }

  // parse students to JSON
  json, err := json.Marshal(students)

  if err != nil {
    fmt.Println("ERROR: parsing students did not work")
    log("GET students/", "500")
    w.WriteHeader(500)
    return
  }

  // return the JSON
  io.WriteString(w, string(json))
  log("GET students/", "200")
}

// add new student
func (s Students) Post(
  w http.ResponseWriter, r *http.Request,
){

  // get next id
  s_next, err := client.Get("next").Result()

  if err != nil {
    fmt.Println("ERROR: 'GET next' did not work")
    log("POST students/", "500")
    w.WriteHeader(500)
    return
  }

  // parse s_next to int
  next, err2 := strconv.Atoi(s_next)

  if err2 != nil {
    fmt.Println("ERROR: parsing next did not work")
    log("POST students/", "500")
    w.WriteHeader(500)
    return
  }

  b := make([]byte, 1024)

  // create new student
  n, err := r.Body.Read(b)

  if err != nil && err != io.EOF {
    fmt.Println("ERROR: Reading HTTP Body did not work")
    log("POST students/", "500")
    w.WriteHeader(500)
    return
  }

  n_student := map[string]interface{}{}

  // parse JSON
  if err := json.Unmarshal(b[:n], &n_student);err != nil {
    fmt.Println("ERROR: JSON parsing did not work")
    log("POST students/", "500")
    w.WriteHeader(500)
    return
  }

  // set new student
  if _, err := client.HMSet(s_next, n_student).Result();
  err != nil {
    fmt.Println("ERROR: HMSET did not work")
    log("POST students/", "500")
    w.WriteHeader(500)
    return
  }

  // increment next
  if _, err := client.Set("next", next + 1, 0).Result();
  err != nil {
    fmt.Println("ERROR: 'SET next' did not work")
    log("POST students/", "500")
    w.WriteHeader(500)
    return
  }

  io.WriteString(w, s_next)
  log("POST students/", "200")
}

func main() {
  // shut producer (global variable) down when process
  // stops
  defer producer.Close()

  // print report whether a message was successful or not
  go func() {
    for e := range producer.Events() {
      switch ev := e.(type) {
      case *kafka.Message:
        if ev.TopicPartition.Error != nil {
          fmt.Printf(
            "ERROR: logging failed: %v\n",
            ev.TopicPartition,
          )
        } else {
          fmt.Printf("SUCCES: %v\n", ev.TopicPartition)
        }
      }
    }
  }()

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
  http.ListenAndServe("0.0.0.0:8080", nil)
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
