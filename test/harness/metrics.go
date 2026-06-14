//go:build functional

package harness

import (
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Scrape fetches the broker's /metrics body. The broker refreshes its gauge metrics
// (partition offsets, group lag) lazily during a scrape, and those fresh values land in
// the *next* response, so Scrape issues a priming request first and returns the second.
func (b *Broker) Scrape() (string, error) {
	if b.MetricsURL == "" {
		return "", fmt.Errorf("metrics URL not configured")
	}
	if _, err := b.scrapeOnce(); err != nil { // prime the gauge refresh
		return "", err
	}
	return b.scrapeOnce()
}

func (b *Broker) scrapeOnce() (string, error) {
	resp, err := http.Get(b.MetricsURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("scrape status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	return string(body), err
}

// CounterValue returns the sample value of the first line in body that starts with
// prefix (e.g. `kafka_produce_requests_total{topic="t"}`), or -1 if absent.
func CounterValue(body, prefix string) float64 {
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, prefix) {
			fields := strings.Fields(line)
			var f float64
			if _, err := fmt.Sscanf(fields[len(fields)-1], "%g", &f); err == nil {
				return f
			}
		}
	}
	return -1
}
