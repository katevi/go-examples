package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()
var rdb *redis.Client

type Message struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func initRedis() {
	// Connect to Redis running in Docker on localhost:6379
	rdb = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // Docker exposes Redis on host's localhost
		Password: "",               // no password set
		DB:       0,                // use default DB
	})

	// Test connection with retry logic
	var err error
	for i := 0; i < 5; i++ {
		_, err = rdb.Ping(ctx).Result()
		if err == nil {
			break
		}
		log.Printf("Failed to connect to Redis (attempt %d/5): %v", i+1, err)
		time.Sleep(2 * time.Second)
	}

	if err != nil {
		log.Fatalf("Could not connect to Redis after 5 attempts: %v", err)
	}

	log.Println("Connected to Redis successfully!")
}

func setHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	err := rdb.Set(ctx, msg.Key, msg.Value, 10*time.Minute).Err()
	if err != nil {
		http.Error(w, fmt.Sprintf("Redis error: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "success",
		"key":    msg.Key,
	})
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Key parameter is required", http.StatusBadRequest)
		return
	}

	val, err := rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, fmt.Sprintf("Redis error: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Message{
		Key:   key,
		Value: val,
	})
}

func getAllHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	keys, err := rdb.Keys(ctx, "*").Result()
	if err != nil {
		http.Error(w, fmt.Sprintf("Redis error: %v", err), http.StatusInternalServerError)
		return
	}

	result := make(map[string]string)
	for _, key := range keys {
		val, err := rdb.Get(ctx, key).Result()
		if err == nil {
			result[key] = val
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

func infoHandler(w http.ResponseWriter, r *http.Request) {
	info, err := rdb.Info(ctx).Result()
	if err != nil {
		http.Error(w, fmt.Sprintf("Redis error: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "Redis Info:\n%s", info)
}

func main() {
	initRedis()

	http.HandleFunc("/set", setHandler)
	http.HandleFunc("/get", getHandler)
	http.HandleFunc("/keys", getAllHandler)
	http.HandleFunc("/info", infoHandler)

	log.Println("Go application starting on :8080")
	log.Println("Redis server should be running in Docker on localhost:6379")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
