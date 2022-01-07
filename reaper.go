package reaper

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gotidy/batch"
)

const (
	collectorKey       = "task_durations"
	batchSize          = 1000
	batchFlushInterval = 10 * time.Second
	flushTimeout       = 60 * time.Second
	popCount           = 1000
)

type Batcher interface {
	Put(v interface{})
	Close()
}

// Handler handles collected messages.
type Handler interface {
	Serve(ctx context.Context, item Item)
}

type HandlerFunc func(ctx context.Context, item Item)

func (f HandlerFunc) Serve(ctx context.Context, item Item) {
	f(ctx, item)
}

// Logger handles errors.
type Logger interface {
	Error(err error)
}

type ErrorFunc func(err error)

func (f ErrorFunc) Error(err error) {
	f(err)
}

type Collector struct {
	redis   *redis.Client
	batcher Batcher
	err     chan error
	cancel  chan struct{}
	handler Handler
	logger  Logger
	wg      sync.WaitGroup
}

type Item struct {
	// Duration seconds
	Duration int
	Labels   []string
}

// Option sets the batcher option.
type Option func(*Collector)

// WithLogger sets logger.
func WithLogger(log Logger) func(*Collector) {
	return func(c *Collector) {
		c.logger = log
	}
}

// WithHandler sets handler of collected data.
func WithHandler(handler Handler) func(*Collector) {
	return func(c *Collector) {
		c.handler = handler
	}
}

func New(redis *redis.Client, opts ...Option) *Collector {
	c := &Collector{
		redis:  redis,
		cancel: make(chan struct{}),
	}

	c.batcher = batch.New(batch.FlusherFunc(c.flush), batch.WithBatchSize(batchSize), batch.WithFlushInterval(batchFlushInterval))

	for _, opt := range opts {
		opt(c)
	}

	if c.handler != nil {
		c.wg.Add(1)
		go c.listen()
	}

	return c
}

func (c *Collector) Close() {
	c.batcher.Close()

	close(c.cancel)
	c.wg.Wait()
}

func (c *Collector) error(err error) {
	if c.logger != nil {
		c.logger.Error(err)
	}
}

func (c *Collector) flush(batch []interface{}) {
	ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
	defer cancel()
	c.push(ctx, batch)
}

func (c *Collector) push(ctx context.Context, items ...interface{}) error {
	if err := c.redis.RPush(ctx, collectorKey, items...).Err(); err != nil {
		return fmt.Errorf("inserting duration info to redis: %w", err)
	}

	return nil
}

func (c *Collector) Push(ctx context.Context, d time.Duration, labels ...string) error {
	b, err := json.Marshal(Item{Duration: int(d.Seconds()), Labels: labels})
	if err != nil {
		return fmt.Errorf("marshaling duration info: %w", err)
	}

	return c.push(ctx, string(b))
}

func (c *Collector) AsyncPush(ctx context.Context, d time.Duration, labels ...string) error {
	b, err := json.Marshal(Item{Duration: int(d.Seconds()), Labels: labels})
	if err != nil {
		return fmt.Errorf("marshaling duration info: %w", err)
	}

	c.batcher.Put(string(b))

	return nil
}

func (c *Collector) get(ctx context.Context, buffer []Item) (count int, err error) {
	res, err := c.redis.LPopCount(ctx, collectorKey, len(buffer)).Result()
	if err != nil {
		return 0, fmt.Errorf("getting all durations info from redis: %w", err)
	}

	for i, s := range res {
		var item Item
		err := json.Unmarshal([]byte(s), &item)
		if err != nil {
			return 0, fmt.Errorf("unmarshaling duration info (%s): %w", s, err)
		}
		buffer[i] = item
	}

	return len(res), nil
}

func (c *Collector) listen() {
	defer c.wg.Done()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-c.cancel:
			cancel()
		}
	}()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}

		// Read all items from the list.
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			res, err := c.redis.LPopCount(ctx, collectorKey, popCount).Result()
			if err != nil && err != redis.Nil {
				c.error(fmt.Errorf("getting all durations info from redis: %w", err))
				continue
			}
			if err == redis.Nil || len(res) == 0 {
				break
			}

			for _, s := range res {
				var item Item
				err := json.Unmarshal([]byte(s), &item)
				if err != nil {
					c.error(fmt.Errorf("unmarshaling duration info (%s): %w", s, err))
				}
				c.handler.Serve(ctx, item)
			}
			if len(res) < popCount {
				break
			}
		}
	}
}

// func (c *Collector) Get(ctx context.Context) ([]Item, error) {
// 	items := make([]Item, 0, len(res))
// 	for _, s := range res {
// 		var item Item
// 		err := json.Unmarshal([]byte(s), &item)
// 		if err != nil {
// 			return nil, fmt.Errorf("unmarshaling duration info (%s): %w", s, err)
// 		}
// 		items = append(items, item)
// 	}

// 	return items, nil
// }

// func (c *Collector) GetAll(ctx context.Context) ([]Item, error) {
// 	c.Get(ctx)
// }
