package queue

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

type StreamPriorityQueue struct {
	client *redis.Client
	ctx    context.Context
	stream string
	group  string
}

func NewStreamPriorityQueue(client *redis.Client, stream, group string) *StreamPriorityQueue {
	pq := &StreamPriorityQueue{
		client: client,
		ctx:    context.Background(),
		stream: stream,
		group:  group,
	}

	// Create consumer group (ignore error if it already exists)
	client.XGroupCreateMkStream(pq.ctx, stream, group, "0").Err()

	return pq
}

func (pq *StreamPriorityQueue) Enqueue(item string, priority int) error {
	return pq.client.XAdd(pq.ctx, &redis.XAddArgs{
		Stream: pq.stream,
		Values: map[string]interface{}{
			"item":     item,
			"priority": strconv.Itoa(priority),
			"created":  strconv.FormatInt(time.Now().UnixNano(), 10),
		},
	}).Err()
}

// Dequeue - Simple blocking dequeue
func (pq *StreamPriorityQueue) Dequeue() (string, int, error) {
	results, err := pq.client.XReadGroup(pq.ctx, &redis.XReadGroupArgs{
		Group:    pq.group,
		Consumer: "consumer-1",
		Streams:  []string{pq.stream, ">"},
		Count:    1,
		Block:    5 * time.Second,
	}).Result()

	if err != nil {
		return "", 0, err
	}

	if len(results) == 0 || len(results[0].Messages) == 0 {
		return "", 0, redis.Nil
	}

	return pq.processMessage(results[0].Messages[0])
}

// DequeueWithPriority - Advanced dequeue with pending message claiming
func (pq *StreamPriorityQueue) DequeueWithPriority() (string, int, error) {
	// 1. First try to claim any pending messages that are stuck
	claimedItem, claimedPriority, err := pq.tryClaimPendingMessages()
	if err == nil {
		return claimedItem, claimedPriority, nil
	}

	// 2. If no pending messages, read new messages
	return pq.Dequeue()
}

func (pq *StreamPriorityQueue) tryClaimPendingMessages() (string, int, error) {
	// Get detailed pending messages
	pendingExt, err := pq.client.XPendingExt(pq.ctx, &redis.XPendingExtArgs{
		Stream:   pq.stream,
		Group:    pq.group,
		Start:    "-",
		End:      "+",
		Count:    10,
		Consumer: "",
	}).Result()

	if err != nil && err != redis.Nil {
		return "", 0, err
	}

	// Find messages that have been idle for more than 30 seconds
	var idleMessageIDs []string
	for _, p := range pendingExt {
		if p.Idle >= 30*time.Second {
			idleMessageIDs = append(idleMessageIDs, p.ID)
		}
	}

	if len(idleMessageIDs) == 0 {
		return "", 0, redis.Nil
	}

	// Claim these messages
	claimed, err := pq.client.XClaim(pq.ctx, &redis.XClaimArgs{
		Stream:   pq.stream,
		Group:    pq.group,
		Consumer: "consumer-1",
		MinIdle:  30 * time.Second,
		Messages: idleMessageIDs,
	}).Result()

	if err != nil || len(claimed) == 0 {
		return "", 0, redis.Nil
	}

	// Process the first claimed message
	return pq.processMessage(claimed[0])
}

func (pq *StreamPriorityQueue) processMessage(msg redis.XMessage) (string, int, error) {
	// Extract values from message
	item, ok := msg.Values["item"].(string)
	if !ok {
		return "", 0, fmt.Errorf("invalid item type")
	}

	priorityStr, ok := msg.Values["priority"].(string)
	if !ok {
		return "", 0, fmt.Errorf("invalid priority type")
	}

	priority, err := strconv.Atoi(priorityStr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid priority value: %v", err)
	}

	// Process the message (your business logic here)
	fmt.Printf("Processing: %s with priority %d\n", item, priority)

	// Acknowledge the message
	err = pq.client.XAck(pq.ctx, pq.stream, pq.group, msg.ID).Err()
	if err != nil {
		return "", 0, fmt.Errorf("failed to ack message: %v", err)
	}

	return item, priority, nil
}

// GetQueueInfo - Helper function to get queue statistics
func (pq *StreamPriorityQueue) GetQueueInfo() (int64, int64, error) {
	// Get total messages in stream
	streamLen, err := pq.client.XLen(pq.ctx, pq.stream).Result()
	if err != nil {
		return 0, 0, err
	}

	// Get pending messages
	pending, err := pq.client.XPending(pq.ctx, pq.stream, pq.group).Result()
	if err != nil && err != redis.Nil {
		return 0, 0, err
	}

	return streamLen, pending.Count, nil
}

// Peek - Look at the next message without removing it
func (pq *StreamPriorityQueue) Peek() (string, int, error) {
	results, err := pq.client.XReadGroup(pq.ctx, &redis.XReadGroupArgs{
		Group:    pq.group,
		Consumer: "peeker",
		Streams:  []string{pq.stream, ">"},
		Count:    1,
		Block:    0,    // Non-blocking
		NoAck:    true, // Don't acknowledge
	}).Result()

	if err != nil {
		return "", 0, err
	}

	if len(results) == 0 || len(results[0].Messages) == 0 {
		return "", 0, redis.Nil
	}

	msg := results[0].Messages[0]
	item, ok := msg.Values["item"].(string)
	if !ok {
		return "", 0, fmt.Errorf("invalid item type")
	}

	priorityStr, ok := msg.Values["priority"].(string)
	if !ok {
		return "", 0, fmt.Errorf("invalid priority type")
	}

	priority, err := strconv.Atoi(priorityStr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid priority value: %v", err)
	}

	return item, priority, nil
}
