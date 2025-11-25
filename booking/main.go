package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/mux"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/segmentio/kafka-go"
)

var (
	db          *sql.DB
	amqpConn    *amqp.Connection
	kafkaReader *kafka.Reader
	kafkaWriter *kafka.Writer
	rabbitURL   string
)

type Booking struct {
	ID           int64     `json:"id"`
	RestaurantID int64     `json:"restaurant_id"`
	User         string    `json:"user"`
	People       int       `json:"people"`
	When         time.Time `json:"when"`
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

	rabbitURL = getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
	amqpConn, err = amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("rabbit dial: %v", err)
	}

	kafkaBroker := getenv("KAFKA_BROKER", "localhost:9092")
	kafkaReader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{kafkaBroker},
		Topic:    "restaurants",
		GroupID:  "booking-restaurants-consumer",
		MinBytes: 1,
		MaxBytes: 10e6,
	})
	kafkaWriter = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaBroker},
		Topic:   "bookings",
	})

	// start background consumer for restaurant events
	go consumeRestaurantEvents()

	r := mux.NewRouter()
	api := r.PathPrefix("/api").Subrouter()
	api.Use(jwtAuthMiddleware)
	api.HandleFunc("/bookings", createBooking).Methods("POST")
	api.HandleFunc("/bookings/{id}", getBooking).Methods("GET")
	api.HandleFunc("/bookings", listBookings).Methods("GET")

	port := getenv("PORT", "8002")
	log.Printf("booking service listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, r))
}

func createBooking(w http.ResponseWriter, r *http.Request) {
	var b Booking
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	// save booking
	_, err := db.Exec("INSERT INTO bookings (restaurant_id, user, people, when_ts) VALUES (?, ?, ?, ?)",
		b.RestaurantID, b.User, b.People, b.When)
	if err != nil {
		log.Printf(err.Error())
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}

	// publish to RabbitMQ queue for email notifications
	go publishRabbitNotification(b)

	// optionally also write to Kafka bookings topic
	//go produceKafkaBooking(b)

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(b)
}

func getBooking(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	var b Booking
	var when time.Time
	err := db.QueryRow("SELECT id, restaurant_id, user, people, when_ts FROM bookings WHERE id = ?", id).
		Scan(&b.ID, &b.RestaurantID, &b.User, &b.People, &when)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	b.When = when
	json.NewEncoder(w).Encode(b)
}

func listBookings(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query("SELECT id, restaurant_id, user, people, when_ts FROM bookings")
	if err != nil {
		http.Error(w, "db error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()
	out := []Booking{}
	for rows.Next() {
		var b Booking
		var when time.Time
		if err := rows.Scan(&b.ID, &b.RestaurantID, &b.User, &b.People, &when); err != nil {
			continue
		}
		b.When = when
		out = append(out, b)
	}
	json.NewEncoder(w).Encode(out)
}

// kafka consumer that listens to restaurant.created / restaurant.deleted events
func consumeRestaurantEvents() {
	for {
		m, err := kafkaReader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("kafka read error: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		var ev map[string]interface{}
		if err := json.Unmarshal(m.Value, &ev); err != nil {
			continue
		}
		typ, _ := ev["type"].(string)
		data := ev["data"]
		if typ == "restaurant.created" {
			// data is restaurant object
			bs, _ := json.Marshal(data)
			var r struct {
				ID    int64  `json:"id"`
				Name  string `json:"name"`
				City  string `json:"city"`
				Seats int    `json:"seats"`
			}
			_ = json.Unmarshal(bs, &r)
			_, _ = db.Exec("REPLACE INTO restaurants_cache (id, name, city, seats, last_seen) VALUES (?, ?, ?, ?, NOW())",
				r.ID, r.Name, r.City, r.Seats)
			log.Printf("cached restaurant %s", r.ID)
		} else if typ == "restaurant.deleted" {
			bs, _ := json.Marshal(data)
			var dd struct {
				ID int64 `json:"id"`
			}
			_ = json.Unmarshal(bs, &dd)
			_, _ = db.Exec("DELETE FROM restaurants_cache WHERE id = ?", dd.ID)
			log.Printf("removed restaurant from cache %s", dd.ID)
		}
	}
}

func publishRabbitNotification(b Booking) {
	ch, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Printf("rabbit dial in publish: %v", err)
		return
	}
	defer ch.Close()
	conn, err := ch.Channel()
	if err != nil {
		log.Printf("rabbit channel: %v", err)
		return
	}
	defer conn.Close()
	q, err := conn.QueueDeclare("email_notifications", true, false, false, false, nil)
	if err != nil {
		log.Printf("queue declare: %v", err)
		return
	}
	payload := map[string]interface{}{
		"type": "booking.created",
		"data": b,
	}
	bs, _ := json.Marshal(payload)
	if err := conn.Publish("", q.Name, false, false, amqp.Publishing{ContentType: "application/json", Body: bs}); err != nil {
		log.Printf("rabbit publish err: %v", err)
	}
}

func produceKafkaBooking(b Booking) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	val, _ := json.Marshal(b)
	_ = kafkaWriter.WriteMessages(ctx, kafka.Message{
		Key:   []byte(strconv.Itoa(int(b.ID))),
		Value: val,
	})
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
