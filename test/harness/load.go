//go:build functional

package harness

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// LoadConfig describes a sustained, real-time-shaped workload: several producers writing
// keyed records across multiple topics at a target rate (with periodic bursts), plus
// consumer groups draining them so consumer-group-lag fluctuates on the dashboards.
type LoadConfig struct {
	Topics     []string      // topics to spread writes across
	Producers  int           // concurrent producer goroutines
	Groups     int           // concurrent consumer groups (each one member)
	RatePerSec int           // approximate total records/sec across all producers
	ValueBytes int           // payload size per record
	Duration   time.Duration // how long to run
	Keyspace   int           // distinct keys (controls partition fan-out)
}

// LoadStats is the outcome of a load run.
type LoadStats struct {
	Produced int64
	Consumed int64
	Errors   int64
}

// DefaultLoad returns a workload sized to make the Grafana panels move without
// overwhelming a laptop.
func DefaultLoad(d time.Duration) LoadConfig {
	return LoadConfig{
		Topics:     []string{"orders", "clicks", "payments"},
		Producers:  4,
		Groups:     2,
		RatePerSec: 2000,
		ValueBytes: 256,
		Duration:   d,
		Keyspace:   500,
	}
}

// RunLoad drives the workload against the broker until the duration elapses, then returns
// aggregate counts. Producers are rate-limited and inject a short burst every few seconds
// so produce-rate and latency graphs show realistic peaks and troughs.
func RunLoad(b *Broker, cfg LoadConfig) (LoadStats, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration)
	defer cancel()

	var st LoadStats
	var wg sync.WaitGroup

	payload := make([]byte, cfg.ValueBytes)
	rand.Read(payload)

	// Per-producer rate so the cluster-wide rate matches RatePerSec.
	perProducer := cfg.RatePerSec / max(cfg.Producers, 1)

	for p := 0; p < cfg.Producers; p++ {
		cl, err := b.Client(kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)))
		if err != nil {
			cancel()
			wg.Wait()
			return st, err
		}
		wg.Add(1)
		go func(id int, cl *kgo.Client) {
			defer wg.Done()
			defer cl.Close()
			produce(ctx, cl, cfg, perProducer, payload, id, &st)
		}(p, cl)
	}

	for g := 0; g < cfg.Groups; g++ {
		cl, err := b.Client(
			kgo.ConsumeTopics(cfg.Topics...),
			kgo.ConsumerGroup(fmt.Sprintf("load-group-%d", g)),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		)
		if err != nil {
			cancel()
			wg.Wait()
			return st, err
		}
		wg.Add(1)
		go func(cl *kgo.Client) {
			defer wg.Done()
			defer cl.Close()
			consume(ctx, cl, &st)
		}(cl)
	}

	wg.Wait()
	return st, nil
}

func produce(ctx context.Context, cl *kgo.Client, cfg LoadConfig, perSec int, payload []byte, id int, st *LoadStats) {
	if perSec <= 0 {
		perSec = 1
	}
	tick := time.NewTicker(time.Second / time.Duration(perSec))
	defer tick.Stop()

	burstEvery := time.NewTicker(5 * time.Second)
	defer burstEvery.Stop()
	burst := 1

	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
	for {
		select {
		case <-ctx.Done():
			cl.Flush(context.Background())
			return
		case <-burstEvery.C:
			// Toggle a 4x burst window so the rate graph isn't flat.
			if burst == 1 {
				burst = 4
			} else {
				burst = 1
			}
		case <-tick.C:
			for i := 0; i < burst; i++ {
				topic := cfg.Topics[rng.Intn(len(cfg.Topics))]
				key := fmt.Sprintf("k-%d", rng.Intn(cfg.Keyspace))
				cl.Produce(ctx, &kgo.Record{
					Topic: topic,
					Key:   []byte(key),
					Value: payload,
				}, func(_ *kgo.Record, err error) {
					if err != nil {
						atomic.AddInt64(&st.Errors, 1)
						return
					}
					atomic.AddInt64(&st.Produced, 1)
				})
			}
		}
	}
}

func consume(ctx context.Context, cl *kgo.Client, st *LoadStats) {
	for ctx.Err() == nil {
		fs := cl.PollFetches(ctx)
		fs.EachError(func(_ string, _ int32, _ error) { atomic.AddInt64(&st.Errors, 1) })
		fs.EachRecord(func(_ *kgo.Record) { atomic.AddInt64(&st.Consumed, 1) })
	}
}
