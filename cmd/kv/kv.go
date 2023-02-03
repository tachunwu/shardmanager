package main

import (
	"runtime"
	"strconv"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

func main() {
	// Enable logger
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// Connect
	nc, _ := nats.Connect(nats.DefaultURL)
	js, _ := nc.JetStream(nats.PublishAsyncMaxPending(256))
	logger.Info("JetStream enable")

	// Create instance
	kv, _ := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket: "bucket",
	})
	logger.Info("JetStream created bucket")

	// 1. Simple Put
	revision, _ := kv.Put("key", []byte("value-0"))
	logger.Info(
		"JetStream simple put",
		zap.String("key:", "key"),
		zap.Binary("value:", []byte("value-0")),
		zap.Uint64("revision:", revision),
	)

	// 2. Simple Get
	entry, _ := kv.Get("key")
	logger.Info(
		"JetStream simple get",
		zap.String("key:", entry.Key()),
		zap.Binary("value:", entry.Value()),
		zap.Uint64("revision:", entry.Revision()),
	)

	// 3. OCC update
	revision, _ = kv.Update("key", []byte("value-1"), entry.Revision())
	logger.Info(
		"JetStream occ update",
		zap.String("key:", "key"),
		zap.Binary("value:", []byte("value-1")),
		zap.Uint64("revision:", revision),
	)

	// 4. Simple Get
	entry, _ = kv.Get("key")
	logger.Info(
		"JetStream simple get",
		zap.String("key:", entry.Key()),
		zap.Uint64("revision:", entry.Revision()),
		zap.Binary("value:", entry.Value()),
	)

	// 5. Simple delete
	kv.Delete("key")
	logger.Info(
		"JetStream simple delete",
		zap.String("key:", entry.Key()),
	)

	// Conclusion:
	// 1. Create bucket first
	// 2. get will have (key, value, revision)
	// 3. occ update need to pass the latest revision

	// Stream name
	name := <-js.StreamNames()
	logger.Info(
		"JetStream kv stream name",
		zap.String("name:", name),
	)

	// Watch
	w, _ := kv.Watch("key")
	go Watching(w, logger)

	// Putting for test waching
	go Putting(kv, logger)

	for {
		runtime.Gosched()
	}
}

func Watching(w nats.KeyWatcher, logger *zap.Logger) {
	defer w.Stop()
	for {
		// Important: event maybe nil so you need to handle it or system will panic
		e := <-w.Updates()
		if e != nil {
			logger.Info(
				"JetStream watch key",
				zap.String("key:", e.Key()),
				zap.ByteString("value:", e.Value()),
			)
		}

	}
}

func Putting(kv nats.KeyValue, logger *zap.Logger) {
	i := 0
	for {
		kv.Put("key", []byte("watch-value-"+strconv.Itoa(i)))
		i++
	}
}
