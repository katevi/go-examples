package main

import (
	"context"
	"fmt"
	"log"

	"github.com/go-redis/redis/v8"
	"kate.redis.example/queue"
)

func main() {
	// Initialize Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	// Test connection
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatal("Redis connection failed:", err)
	}

	// Create priority queue
	pq := queue.NewStreamPriorityQueue(rdb, "my_priority_stream", "worker_group")

	// Enqueue some items with different priorities
	fmt.Println("Enqueueing items...")
	items := []struct {
		item     string
		priority int
	}{
		{"critical_task", 1},
		{"important_task", 2},
		{"normal_task", 3},
		{"low_priority_task", 4},
	}

	for _, item := range items {
		err := pq.Enqueue(item.item, item.priority)
		if err != nil {
			log.Printf("Failed to enqueue %s: %v", item.item, err)
		} else {
			fmt.Printf("Enqueued: %s (priority %d)\n", item.item, item.priority)
		}
	}

	// Get queue info
	total, pending, err := pq.GetQueueInfo()
	if err != nil {
		log.Printf("Failed to get queue info: %v", err)
	} else {
		fmt.Printf("Queue info - Total: %d, Pending: %d\n", total, pending)
	}

	// Peek at the next message
	item, priority, err := pq.Peek()
	if err == nil {
		fmt.Printf("Next message: %s (priority %d)\n", item, priority)
	}

	// Dequeue and process messages
	fmt.Println("\nDequeueing messages...")
	for i := 0; i < 5; i++ {
		item, priority, err := pq.Dequeue()
		if err != nil {
			if err == redis.Nil {
				fmt.Println("No more messages in queue")
				break
			}
			log.Printf("Dequeue error: %v", err)
			break
		}
		fmt.Printf("Dequeued: %s (priority %d)\n", item, priority)
	}

	// Demonstrate advanced dequeue with claiming
	fmt.Println("\nTrying advanced dequeue...")
	item, priority, err = pq.DequeueWithPriority()
	if err != nil {
		if err == redis.Nil {
			fmt.Println("No messages available")
		} else {
			log.Printf("Advanced dequeue error: %v", err)
		}
	} else {
		fmt.Printf("Advanced dequeue got: %s (priority %d)\n", item, priority)
	}
}
