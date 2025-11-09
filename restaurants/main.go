package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	_ "time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
	"github.com/segmentio/kafka-go"
)

var (
	db          *sql.DB
	kafkaBroker string
	kWriter     *kafka.Writer
)

type Restaurant struct {
	ID    int64  `json:"id"`
	Name  string `json:"name"`
	City  string `json:"city"`
	Seats int    `json:"seats"`
}

func main() {
	dsn := getenv("MYSQL_DSN", "root:rootpass@tcp(mysql:3306)/microdb?parseTime=true")
	var err error
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("db open: %v", err)
	}
	if err := db.Ping(); err != nil {
		log.Fatalf("db ping: %v", err)
	}
	kafkaBroker = getenv("KAFKA_BROKER", "localhost:9092")
	kWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    "restaurants",
		Balancer: &kafka.LeastBytes{},
	})

	r := mux.NewRouter()
	api := r.PathPrefix("/api").Subrouter()
	api.HandleFunc("/restaurants", listRestaurants).Methods("GET")
	api.HandleFunc("/restaurants", createRestaurant).Methods("POST")
	api.HandleFunc("/restaurants/{id}", getRestaurant).Methods("GET")
	api.HandleFunc("/restaurants/{id}", deleteRestaurant).Methods("DELETE")

	port := getenv("PORT", "8001")
	log.Printf("restaurants service listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, r))
}

func listRestaurants(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query("SELECT id, name, city, seats FROM restaurants")
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	out := []Restaurant{}
	for rows.Next() {
		var rr Restaurant
		if err := rows.Scan(&rr.ID, &rr.Name, &rr.City, &rr.Seats); err != nil {
			continue
		}
		out = append(out, rr)
	}
	json.NewEncoder(w).Encode(out)
}

func createRestaurant(w http.ResponseWriter, r *http.Request) {
	var rr Restaurant
	if err := json.NewDecoder(r.Body).Decode(&rr); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	_, err := db.Exec("INSERT INTO restaurants (name, city, seats) VALUES (?, ?, ?)", rr.Name, rr.City, rr.Seats)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	// publish kafka event: restaurant.created
	event := map[string]interface{}{
		"type": "restaurant.created",
		"data": rr,
	}
	b, _ := json.Marshal(event)
	_ = kWriter.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(strconv.Itoa(int(rr.ID))),
		Value: b,
	})
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(rr)
}

func getRestaurant(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	var rr Restaurant
	err := db.QueryRow("SELECT id, name, city, seats FROM restaurants WHERE id = ?", id).Scan(&rr.ID, &rr.Name, &rr.City, &rr.Seats)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	json.NewEncoder(w).Encode(rr)
}

func deleteRestaurant(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	_, err := db.Exec("DELETE FROM restaurants WHERE id = ?", id)
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	// publish kafka event: restaurant.deleted
	event := map[string]interface{}{
		"type": "restaurant.deleted",
		"data": map[string]string{"id": id},
	}
	b, _ := json.Marshal(event)
	_ = kWriter.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(id),
		Value: b,
	})
	w.WriteHeader(http.StatusNoContent)
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
