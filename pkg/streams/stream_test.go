package streams

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// =============================================================================
// Embedded NATS Server Helper
// =============================================================================

// startEmbeddedNATS starts an embedded NATS server with JetStream enabled
func startEmbeddedNATS(t testing.TB) (*server.Server, string) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "nats-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	opts := &server.Options{
		Host:           "127.0.0.1",
		Port:           -1, // Random available port
		NoLog:          true,
		NoSigs:         true,
		MaxControlLine: 4096,
		JetStream:      true,
		StoreDir:       tmpDir,
	}

	ns, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("Failed to create NATS server: %v", err)
	}

	go ns.Start()

	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS server not ready")
	}

	t.Cleanup(func() {
		ns.Shutdown()
		os.RemoveAll(tmpDir)
	})

	return ns, ns.ClientURL()
}

// publishTestMessages publishes N messages with subjects like:
// published.core.webclient.content.entries.page.<entryID>.<market>
func publishTestMessages(t testing.TB, nc *nats.Conn, streamName string, count int) {
	t.Helper()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Failed to create JetStream context: %v", err)
	}

	ctx := context.Background()

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:              streamName,
		Subjects:          []string{"published.>"},
		MaxMsgsPerSubject: 1,
		Storage:           jetstream.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	markets := []string{"eu", "at", "no", "ca", "ch", "eu1", "eu2", "de", "fi", "nz", "row", "default"}

	for i := 0; i < count; i++ {
		entryID := fmt.Sprintf("entry%08d", i)
		market := markets[i%len(markets)]
		subject := fmt.Sprintf("published.core.webclient.content.entries.page.%s.%s", entryID, market)
		data := []byte(fmt.Sprintf(`{"id":"%s","market":"%s","index":%d}`, entryID, market, i))

		_, err := js.Publish(ctx, subject, data)
		if err != nil {
			t.Fatalf("Failed to publish message %d: %v", i, err)
		}
	}
}

// =============================================================================
// Tests with Embedded NATS (self-contained, no external dependencies)
// =============================================================================

// TestLastPerSubjectPerformance tests LastPerSubject with various message counts
func TestLastPerSubjectPerformance(t *testing.T) {
	testCases := []struct {
		name    string
		count   int
		entryID string
	}{
		{"1 message", 1, "entry00000000"},
		{"12 messages (like production)", 12, ""},
		{"100 messages", 100, ""},
		{"1000 messages", 1000, ""},
		{"10000 messages", 10000, ""},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, natsURL := startEmbeddedNATS(t)
			streamName := "published"

			nc, err := nats.Connect(natsURL)
			if err != nil {
				t.Fatalf("Failed to connect: %v", err)
			}
			defer nc.Close()

			publishTestMessages(t, nc, streamName, tc.count)

			stream, err := NewStream(natsURL, streamName)
			if err != nil {
				t.Fatalf("Failed to create stream: %v", err)
			}

			var filter string
			if tc.entryID != "" {
				filter = fmt.Sprintf("published.core.webclient.content.entries.page.%s.*", tc.entryID)
			} else {
				filter = "published.core.webclient.content.entries.page.>"
			}

			start := time.Now()
			result, err := stream.LastPerSubject([]string{filter})
			elapsed := time.Since(start)

			if err != nil {
				t.Fatalf("LastPerSubject failed: %v", err)
			}

			expectedCount := tc.count
			if tc.entryID != "" {
				expectedCount = 1
			}

			if len(result) != expectedCount {
				t.Errorf("Expected %d messages, got %d", expectedCount, len(result))
			}

			t.Logf("✓ %d messages fetched in %v (%.2f msgs/sec)",
				len(result), elapsed, float64(len(result))/elapsed.Seconds())
		})
	}
}

// TestLastPerSubjectCorrectness verifies the function returns correct data
func TestLastPerSubjectCorrectness(t *testing.T) {
	_, natsURL := startEmbeddedNATS(t)
	streamName := "published"

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Failed to create JetStream: %v", err)
	}

	ctx := context.Background()

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:              streamName,
		Subjects:          []string{"published.>"},
		MaxMsgsPerSubject: 1,
		Storage:           jetstream.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	testData := map[string]string{
		"published.core.webclient.content.entries.page.abc123.eu":      `{"id":"abc123","market":"eu"}`,
		"published.core.webclient.content.entries.page.abc123.us":      `{"id":"abc123","market":"us"}`,
		"published.core.webclient.content.entries.page.def456.eu":      `{"id":"def456","market":"eu"}`,
		"published.core.webclient.content.entries.page.ghi789.default": `{"id":"ghi789","market":"default"}`,
	}

	for subject, data := range testData {
		_, err := js.Publish(ctx, subject, []byte(data))
		if err != nil {
			t.Fatalf("Failed to publish: %v", err)
		}
	}

	stream, err := NewStream(natsURL, streamName)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	result, err := stream.LastPerSubject([]string{"published.core.webclient.content.entries.page.>"})
	if err != nil {
		t.Fatalf("LastPerSubject failed: %v", err)
	}

	if len(result) != len(testData) {
		t.Errorf("Expected %d subjects, got %d", len(testData), len(result))
	}

	for subject, expectedData := range testData {
		messages, ok := result[subject]
		if !ok {
			t.Errorf("Missing subject: %s", subject)
			continue
		}
		if len(messages) != 1 {
			t.Errorf("Expected 1 message for %s, got %d", subject, len(messages))
			continue
		}
		if string(messages[0]) != expectedData {
			t.Errorf("Data mismatch for %s: expected %s, got %s", subject, expectedData, string(messages[0]))
		}
	}

	t.Logf("✓ All %d messages verified correctly", len(testData))
}

// TestLastPerSubjectWithSpecificEntryFilter tests filtering by specific entry ID (like production)
func TestLastPerSubjectWithSpecificEntryFilter(t *testing.T) {
	_, natsURL := startEmbeddedNATS(t)
	streamName := "published"

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Failed to create JetStream: %v", err)
	}

	ctx := context.Background()

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:              streamName,
		Subjects:          []string{"published.>"},
		MaxMsgsPerSubject: 1,
		Storage:           jetstream.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	// Publish messages for multiple entry IDs with multiple markets each
	entryIDs := []string{"d5nn8u2ilccs73ajs3m0", "abc123", "def456"}
	markets := []string{"eu", "at", "no", "ca", "ch", "eu1", "eu2", "de", "fi", "nz", "row", "default"}

	for _, entryID := range entryIDs {
		for _, market := range markets {
			subject := fmt.Sprintf("published.core.webclient.content.entries.page.%s.%s", entryID, market)
			data := fmt.Sprintf(`{"id":"%s","market":"%s"}`, entryID, market)
			_, err := js.Publish(ctx, subject, []byte(data))
			if err != nil {
				t.Fatalf("Failed to publish: %v", err)
			}
		}
	}

	stream, err := NewStream(natsURL, streamName)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	// Test filtering by specific entry ID (like production benchmark)
	filter := "published.core.webclient.content.entries.page.d5nn8u2ilccs73ajs3m0.*"

	start := time.Now()
	result, err := stream.LastPerSubject([]string{filter})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("LastPerSubject failed: %v", err)
	}

	expectedCount := len(markets)
	if len(result) != expectedCount {
		t.Errorf("Expected %d messages, got %d", expectedCount, len(result))
	}

	t.Logf("✓ Fetched %d messages in %v for entry ID filter", len(result), elapsed)
}

// TestStreamConnect tests basic stream connection with embedded NATS
func TestStreamConnect(t *testing.T) {
	_, natsURL := startEmbeddedNATS(t)

	stream, err := NewStream(natsURL, "test_stream")
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	_, err = stream.CreateStream([]string{"test.>"})
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	t.Log("✓ Stream connection successful")
}

// TestStreamPublishAndConsume tests basic publish/consume with embedded NATS
func TestStreamPublishAndConsume(t *testing.T) {
	_, natsURL := startEmbeddedNATS(t)
	streamName := "test_stream"
	subject := "test.messages"
	publishCount := 10

	stream, err := NewStream(natsURL, streamName)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	_, err = stream.CreateStream([]string{"test.>"})
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	// Publish messages
	for i := 0; i < publishCount; i++ {
		_, err = stream.Publish(subject, []byte(fmt.Sprintf("message-%d", i)))
		if err != nil {
			t.Fatalf("Failed to publish: %v", err)
		}
	}

	// Consume messages
	wg := &sync.WaitGroup{}
	wg.Add(publishCount)

	consumer, err := stream.CreateConsumer(streamName, "test_consumer")
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	received := 0
	cc, err := consumer.Consume(func(msg jetstream.Msg) {
		msg.Ack()
		received++
		wg.Done()
	})
	if err != nil {
		t.Fatalf("Failed to consume: %v", err)
	}
	defer cc.Stop()

	// Wait with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Logf("✓ Received %d messages", received)
	case <-time.After(5 * time.Second):
		t.Fatalf("Timeout waiting for messages, received %d/%d", received, publishCount)
	}
}

// TestKeyValue tests KV operations with embedded NATS
func TestKeyValue(t *testing.T) {
	_, natsURL := startEmbeddedNATS(t)
	bucket := "testbucket"
	key := "testkey"
	val := "testval"

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Failed to create JetStream: %v", err)
	}

	ctx := context.Background()

	kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: bucket,
	})
	if err != nil {
		t.Fatalf("Failed to create KV bucket: %v", err)
	}

	_, err = kv.Create(ctx, key, []byte(val))
	if err != nil {
		t.Fatalf("Failed to create key: %v", err)
	}

	entry, err := kv.Get(ctx, key)
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}

	if string(entry.Value()) != val {
		t.Errorf("Expected %s, got %s", val, string(entry.Value()))
	}

	t.Logf("✓ KV operations successful")
}

// =============================================================================
// Benchmarks
// =============================================================================

// BenchmarkLastPerSubject benchmarks LastPerSubject with different message counts
func BenchmarkLastPerSubject(b *testing.B) {
	messageCounts := []int{1, 10, 100, 1000, 10000, 100000}

	for _, count := range messageCounts {
		b.Run(fmt.Sprintf("%d_messages", count), func(b *testing.B) {
			_, natsURL := startEmbeddedNATS(b)
			streamName := "published"

			nc, err := nats.Connect(natsURL)
			if err != nil {
				b.Fatalf("Failed to connect: %v", err)
			}
			defer nc.Close()

			publishTestMessages(b, nc, streamName, count)

			stream, err := NewStream(natsURL, streamName)
			if err != nil {
				b.Fatalf("Failed to create stream: %v", err)
			}

			filter := "published.core.webclient.content.entries.page.>"

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := stream.LastPerSubject([]string{filter})
				if err != nil {
					b.Fatalf("LastPerSubject failed: %v", err)
				}
				if len(result) != count {
					b.Errorf("Expected %d messages, got %d", count, len(result))
				}
			}
		})
	}
}

// BenchmarkLastPerSubjectLarge benchmarks with very large message counts
func BenchmarkLastPerSubjectLarge(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping large benchmark in short mode")
	}

	messageCounts := []int{100000, 500000, 1000000}

	for _, count := range messageCounts {
		b.Run(fmt.Sprintf("%d_messages", count), func(b *testing.B) {
			_, natsURL := startEmbeddedNATS(b)
			streamName := "published"

			nc, err := nats.Connect(natsURL)
			if err != nil {
				b.Fatalf("Failed to connect: %v", err)
			}
			defer nc.Close()

			b.Logf("Publishing %d messages...", count)
			publishTestMessages(b, nc, streamName, count)
			b.Logf("Publishing complete")

			stream, err := NewStream(natsURL, streamName)
			if err != nil {
				b.Fatalf("Failed to create stream: %v", err)
			}

			filter := "published.core.webclient.content.entries.page.>"

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, err := stream.LastPerSubject([]string{filter})
				if err != nil {
					b.Fatalf("LastPerSubject failed: %v", err)
				}
				if len(result) != count {
					b.Errorf("Expected %d messages, got %d", count, len(result))
				}
			}
		})
	}
}

// TestFetchAll tests FetchAll with time-based filtering
func TestFetchAll(t *testing.T) {
	_, natsURL := startEmbeddedNATS(t)
	streamName := "test_stream"

	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Failed to create JetStream: %v", err)
	}

	ctx := context.Background()

	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     streamName,
		Subjects: []string{"transaction.>"},
		Storage:  jetstream.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	// Publish test messages
	for i := 0; i < 10; i++ {
		subject := fmt.Sprintf("transaction.user%d.balance_transaction", i)
		data := fmt.Sprintf(`{"user":%d,"amount":100}`, i)
		_, err := js.Publish(ctx, subject, []byte(data))
		if err != nil {
			t.Fatalf("Failed to publish: %v", err)
		}
	}

	stream, err := NewStream(natsURL, streamName)
	if err != nil {
		t.Fatalf("Failed to create stream: %v", err)
	}

	// Test FetchAll without time filter
	result, err := stream.FetchAll([]string{"transaction.*.balance_transaction"}, nil)
	if err != nil {
		t.Fatalf("FetchAll failed: %v", err)
	}

	if len(result) != 10 {
		t.Errorf("Expected 10 messages, got %d", len(result))
	}

	// Test FetchAll with time filter (future time should return 0)
	futureTime := time.Now().Add(1 * time.Hour)
	result, err = stream.FetchAll([]string{"transaction.*.balance_transaction"}, &futureTime)
	if err != nil {
		t.Fatalf("FetchAll with time filter failed: %v", err)
	}

	if len(result) != 0 {
		t.Errorf("Expected 0 messages with future time filter, got %d", len(result))
	}

	t.Logf("✓ FetchAll operations successful")
}
