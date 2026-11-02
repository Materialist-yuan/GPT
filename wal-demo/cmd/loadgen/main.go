// Command loadgen measures the demo HTTP API and checks final asset balances.
// Run from wal-demo: go run ./cmd/loadgen -url http://127.0.0.1:8081
// Run the same command against :8080 to measure the synchronous baseline.
// APP_API_TOKEN supplies the bearer token; it is never included in the report.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type outcome struct {
	attempted bool
	status    int
	success   bool
	network   bool
	protocol  bool
	latency   time.Duration
	coins     int64
}

type metrics struct {
	Attempted              int            `json:"attempted"`
	Success                int            `json:"success"`
	NetworkErrors          int            `json:"network_errors"`
	ProtocolErrors         int            `json:"protocol_errors"`
	HTTPStatuses           map[string]int `json:"http_statuses"`
	ElapsedSeconds         float64        `json:"elapsed_seconds"`
	AttemptsPerSecond      float64        `json:"attempts_per_second"`
	SuccessfulOpsPerSecond float64        `json:"successful_ops_per_second"`
	P50Milliseconds        float64        `json:"p50_ms"`
	P95Milliseconds        float64        `json:"p95_ms"`
	P99Milliseconds        float64        `json:"p99_ms"`
}

type report struct {
	Players           int      `json:"players"`
	Requested         int      `json:"requested"`
	Concurrency       int      `json:"concurrency"`
	Create            metrics  `json:"create"`
	Rewards           *metrics `json:"rewards,omitempty"`
	ValidationReads   *metrics `json:"validation_reads,omitempty"`
	BalanceMismatches int      `json:"balance_mismatches"`
	ValidationPassed  bool     `json:"validation_passed"`
	Passed            bool     `json:"passed"`
	Note              string   `json:"note"`
}

type client struct {
	http  *http.Client
	base  string
	token string
}

func main() { os.Exit(run()) }

func run() int {
	base := flag.String("url", "http://127.0.0.1:8081", "HTTP API base URL")
	players := flag.Int("players", 100, "players to create (1..10000)")
	requests := flag.Int("requests", 2000, "reward requests (1..1000000)")
	concurrency := flag.Int("concurrency", 32, "bounded HTTP workers (1..1024)")
	flag.Parse()
	u, err := url.Parse(*base)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" ||
		*players < 1 || *players > 10000 || *requests < 1 || *requests > 1000000 || *concurrency < 1 || *concurrency > 1024 || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "invalid flags: use -help; URL must be HTTP(S) without credentials, query or fragment")
		return 2
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment, MaxIdleConns: *concurrency * 2,
		MaxIdleConnsPerHost: *concurrency, MaxConnsPerHost: *concurrency,
		IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: 5 * time.Second,
	}
	defer transport.CloseIdleConnections()
	c := client{http: &http.Client{Transport: transport, Timeout: 10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		base: strings.TrimRight(*base, "/"), token: os.Getenv("APP_API_TOKEN")}
	r := report{Players: *players, Requested: *requests, Concurrency: *concurrency,
		Note: "Closed-loop single-host HTTP workload. Latency includes failed attempts. No automatic retries; timeouts may have committed. Not a cluster capacity claim."}
	prefix := "bench_" + strconv.FormatInt(time.Now().UnixNano(), 36) + "_"
	ids := make([]string, *players)
	for i := range ids {
		ids[i] = prefix + strconv.Itoa(i)
	}
	started := time.Now()
	created := parallel(ctx, *players, *concurrency, func(i int) outcome {
		return c.request(ctx, "/players", map[string]any{"player_id": ids[i], "request_id": "create"}, ids[i], 201)
	})
	r.Create = summarize(created, time.Since(started))
	if r.Create.Success != *players {
		return finish(r)
	}
	started = time.Now()
	rewards := parallel(ctx, *requests, *concurrency, func(i int) outcome {
		id := ids[i%*players]
		return c.request(ctx, "/players/"+id+"/rewards", map[string]any{"request_id": "op_" + strconv.Itoa(i), "amount": 1}, id, 200)
	})
	m := summarize(rewards, time.Since(started))
	r.Rewards = &m
	expected := make([]int64, *players)
	for i := range expected {
		expected[i] = 100
	}
	for i, result := range rewards {
		if result.success {
			expected[i%*players]++
		}
	}
	started = time.Now()
	reads := parallel(ctx, *players, *concurrency, func(i int) outcome {
		return c.request(ctx, "/players/"+ids[i], nil, ids[i], 200)
	})
	readMetrics := summarize(reads, time.Since(started))
	r.ValidationReads = &readMetrics
	for i, result := range reads {
		if result.success && result.coins != expected[i] {
			r.BalanceMismatches++
		}
	}
	r.ValidationPassed = readMetrics.Success == *players && r.BalanceMismatches == 0
	r.Passed = r.ValidationPassed && m.Success == *requests
	return finish(r)
}

func finish(r report) int {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(r); err != nil {
		return 1
	}
	if !r.Passed {
		return 1
	}
	return 0
}

func parallel(ctx context.Context, count, workers int, run func(int) outcome) []outcome {
	results := make([]outcome, count)
	jobs := make(chan int)
	var wg sync.WaitGroup
	if workers > count {
		workers = count
	}
	for n := 0; n < workers; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				results[i] = run(i)
			}
		}()
	}
dispatch:
	for i := 0; i < count; i++ {
		select {
		case <-ctx.Done():
			break dispatch
		case jobs <- i:
		}
	}
	close(jobs)
	wg.Wait()
	return results
}

func (c client) request(ctx context.Context, path string, body any, id string, expectedStatus int) (result outcome) {
	result.attempted = true
	start := time.Now()
	defer func() { result.latency = time.Since(start) }()
	method := http.MethodGet
	var reader io.Reader
	if body != nil {
		method = http.MethodPost
		encoded, err := json.Marshal(body)
		if err != nil {
			result.protocol = true
			return
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, reader)
	if err != nil {
		result.protocol = true
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		result.network = true
		return
	}
	defer resp.Body.Close()
	result.status = resp.StatusCode
	if result.status != expectedStatus {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 65536))
		return
	}
	var decoded struct {
		ID     string `json:"id"`
		Coins  int64  `json:"coins"`
		Code   string `json:"code"`
		Player *struct {
			ID    string `json:"id"`
			Coins int64  `json:"coins"`
		} `json:"player"`
	}
	decoder := json.NewDecoder(io.LimitReader(resp.Body, 65536))
	if err := decoder.Decode(&decoded); err != nil {
		result.protocol = true
		return
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		result.protocol = true
		return
	}
	if body == nil {
		result.success = decoded.ID == id
		result.coins = decoded.Coins
	} else {
		result.success = decoded.Code == "ok" && decoded.Player != nil && decoded.Player.ID == id
		if decoded.Player != nil {
			result.coins = decoded.Player.Coins
		}
	}
	result.protocol = !result.success
	return
}

func summarize(results []outcome, elapsed time.Duration) metrics {
	m := metrics{ElapsedSeconds: elapsed.Seconds(), HTTPStatuses: make(map[string]int)}
	latencies := make([]time.Duration, 0, len(results))
	for _, r := range results {
		if !r.attempted {
			continue
		}
		m.Attempted++
		latencies = append(latencies, r.latency)
		if r.success {
			m.Success++
		}
		if r.network {
			m.NetworkErrors++
		}
		if r.protocol {
			m.ProtocolErrors++
		}
		if r.status != 0 {
			m.HTTPStatuses[strconv.Itoa(r.status)]++
		}
	}
	if elapsed > 0 {
		m.AttemptsPerSecond = float64(m.Attempted) / elapsed.Seconds()
		m.SuccessfulOpsPerSecond = float64(m.Success) / elapsed.Seconds()
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	percentile := func(percent int) float64 {
		if len(latencies) == 0 {
			return 0
		}
		index := (len(latencies)*percent+99)/100 - 1
		return float64(latencies[index]) / float64(time.Millisecond)
	}
	m.P50Milliseconds, m.P95Milliseconds, m.P99Milliseconds = percentile(50), percentile(95), percentile(99)
	return m
}
