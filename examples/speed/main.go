package main

import (
	"context"
	"math/rand"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gotidy/reaper"
	"github.com/rs/zerolog"
)

func RedisClient() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
}

func logger() zerolog.Logger {
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
	// output.FormatLevel = func(i interface{}) string {
	// 	return strings.ToUpper(fmt.Sprintf("| %-6s|", i))
	// }
	// output.FormatMessage = func(i interface{}) string {
	// 	return fmt.Sprintf("***%s****", i)
	// }
	// output.FormatFieldName = func(i interface{}) string {
	// 	return fmt.Sprintf("%s:", i)
	// }
	// output.FormatFieldValue = func(i interface{}) string {
	// 	return strings.ToUpper(fmt.Sprintf("%s", i))
	// }

	return zerolog.New(output).With().Timestamp().Logger()
}

func run(log zerolog.Logger, async bool) {
	redisClient := RedisClient()
	if err := redisClient.Ping(context.TODO()).Err(); err != nil {
		log.Fatal().Err(err).Msg("redis ping failed")
	}
	log.Info().Msg("redis successful pinged")

	received := 0
	collector := reaper.New(redisClient,
		reaper.WithHandler(reaper.HandlerFunc(func(ctx context.Context, item reaper.Item) {
			received++
		})),
		reaper.WithLogger(reaper.ErrorFunc(func(err error) {
			log.Error().Err(err).Msg("collector error")
		})),
	)

	labels := []string{"label-some1", "label-some2", "label-some3", "label-some4", "label-some5", "label-some6"}
	rndValues := rand.New(rand.NewSource(10001))
	rndLabels := rand.New(rand.NewSource(3))
	count := 100000

	start := time.Now()
	for i := 0; i < count; i++ {
		duration := time.Duration(60.0+5*24*60*60*rndValues.Float64()) / time.Second
		if !async {
			if err := collector.Push(context.TODO(), duration, labels[rndLabels.Int()%len(labels)]); err != nil {
				log.Fatal().Err(err).Msg("pushing duration failed")
			}
		} else {
			if err := collector.AsyncPush(context.TODO(), duration, labels[rndLabels.Int()%len(labels)]); err != nil {
				log.Fatal().Err(err).Msg("pushing duration failed")
			}
		}
	}
	dur := time.Since(start)

	time.Sleep(time.Second * 2)

	collector.Close()

	log.Info().Dur("duration", dur).Str("operation", "pushing").Int("count", count).Int("received", received).Dur("per_op", dur/time.Duration(count)).Msg("pushing durations")
}

func main() {
	log := logger()

	log.Info().Msg("Sync")
	run(log, false)

	log.Info().Msg("Async")
	run(log, true)
}
