package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

type StatsCounter struct {
	rdb *redis.Client
	ctx context.Context
}

func NewStatsCounter(rdb *redis.Client) *StatsCounter {
	return &StatsCounter{
		rdb: rdb,
		ctx: context.Background(),
	}
}

// Track basic page view
func (sc *StatsCounter) TrackPageView(page string, userID string) error {
	now := time.Now()

	_, err := sc.rdb.Pipelined(sc.ctx, func(pipe redis.Pipeliner) error {
		// Total views
		pipe.Incr(sc.ctx, fmt.Sprintf("stats:page:%s:total", page))

		// Daily views
		dailyKey := fmt.Sprintf("stats:page:%s:%s", page, now.Format("2006-01-02"))
		pipe.Incr(sc.ctx, dailyKey)
		pipe.Expire(sc.ctx, dailyKey, 48*time.Hour) // Keep for 2 days

		// Hourly views (for real-time analytics)
		hourlyKey := fmt.Sprintf("stats:page:%s:%s", page, now.Format("2006-01-02-15"))
		pipe.Incr(sc.ctx, hourlyKey)
		pipe.Expire(sc.ctx, hourlyKey, 48*time.Hour)

		// Unique visitors using HyperLogLog
		if userID != "" {
			pipe.PFAdd(sc.ctx, fmt.Sprintf("stats:page:%s:unique_visitors", page), userID)
		}

		return nil
	})

	return err
}

// Get page statistics
func (sc *StatsCounter) GetPageStats(page string) (map[string]interface{}, error) {
	cmds, err := sc.rdb.Pipelined(sc.ctx, func(pipe redis.Pipeliner) error {
		pipe.Get(sc.ctx, fmt.Sprintf("stats:page:%s:total", page))
		pipe.Get(sc.ctx, fmt.Sprintf("stats:page:%s:%s", page, time.Now().Format("2006-01-02")))
		pipe.PFCount(sc.ctx, fmt.Sprintf("stats:page:%s:unique_visitors", page))
		return nil
	})

	if err != nil && err != redis.Nil {
		return nil, err
	}

	stats := map[string]interface{}{
		"total_views":     cmds[0].(*redis.StringCmd).Val(),
		"today_views":     cmds[1].(*redis.StringCmd).Val(),
		"unique_visitors": cmds[2].(*redis.IntCmd).Val(),
	}

	return stats, nil
}

func main() {
	ctx := context.Background()

	// Initialize Redis
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	// Test connection
	if _, err := rdb.Ping(ctx).Result(); err != nil {
		log.Fatal("❌ Redis connection failed:", err)
	}
	fmt.Println("✅ Redis connected!")

	statsCounter := NewStatsCounter(rdb)

	// HTTP Handlers
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"message": "Redis Stats API",
			"endpoints": `
GET  /stats           - Get all statistics
GET  /stats/{page}    - Get stats for specific page
POST /click/{page}    - Simulate page click
POST /clear           - Clear all statistics
			`,
		})
	})

	http.HandleFunc("/stats/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		page := r.URL.Path[len("/stats/"):]
		if page == "" {
			http.Error(w, "Page name required", http.StatusBadRequest)
			return
		}

		stats, err := statsCounter.GetPageStats(page)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(stats)
	})

	http.HandleFunc("/click/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		page := r.URL.Path[len("/click/"):]
		if page == "" {
			http.Error(w, "Page name required", http.StatusBadRequest)
			return
		}

		userID := uuid.New().String()
		err := statsCounter.TrackPageView(page, userID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "success",
			"message": fmt.Sprintf("Clicked %s page", page),
			"page":    page,
		})
	})

	// Start server
	port := ":8080"
	fmt.Printf("🚀 Server starting on http://localhost%s\n", port)
	fmt.Println("📊 Available endpoints:")
	fmt.Println("   GET  http://localhost:8080/stats")
	fmt.Println("   GET  http://localhost:8080/stats/home")
	fmt.Println("   POST http://localhost:8080/click/about")
	fmt.Println("   POST http://localhost:8080/clear")

	log.Fatal(http.ListenAndServe(port, nil))
}
