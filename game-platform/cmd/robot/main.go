package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"gameplatform/internal/loadtest"
)

func main() {
	os.Exit(run())
}

func run() int {
	cfg := loadtest.DefaultConfig()
	if value, ok := os.LookupEnv("ROBOT_LOGIN_URL"); ok {
		cfg.LoginURL = value
	}
	if value, ok := os.LookupEnv("ROBOT_GATEWAY"); ok {
		cfg.Gateway = value
	}
	if value, ok := os.LookupEnv("ROBOT_SERVER_ID"); ok {
		cfg.ServerID = value
	}
	var reportPath string
	flag.StringVar(&cfg.LoginURL, "login-url", cfg.LoginURL, "login service http(s) origin")
	flag.StringVar(&cfg.Gateway, "gateway", cfg.Gateway, "TCP host:port override; empty uses ticket response")
	flag.StringVar(&cfg.ServerID, "server-id", cfg.ServerID, "game server ID")
	flag.IntVar(&cfg.Bots, "bots", cfg.Bots, "number of distinct accounts and TCP connections")
	flag.DurationVar(&cfg.Duration, "duration", cfg.Duration, "total gateway phase duration including ramp")
	flag.DurationVar(&cfg.Ramp, "ramp", cfg.Ramp, "spread connection starts over this interval")
	flag.Float64Var(&cfg.Rate, "rate", cfg.Rate, "messages per second per bot; one request in flight")
	flag.IntVar(&cfg.PayloadBytes, "payload-bytes", cfg.PayloadBytes, "echo data length before JSON wrapper")
	flag.IntVar(&cfg.PrepareConcurrency, "prepare-concurrency", cfg.PrepareConcurrency, "parallel account preparation workers")
	flag.StringVar(&cfg.Prefix, "prefix", cfg.Prefix, "unique account prefix (default random)")
	flag.DurationVar(&cfg.Timeout, "timeout", cfg.Timeout, "per-operation HTTP/TCP timeout")
	flag.BoolVar(&cfg.TLS, "tls", false, "use TLS1.3 for TCP gateway")
	flag.StringVar(&cfg.TLSCA, "tls-ca", "", "optional extra trusted PEM CA file for TCP gateway")
	flag.StringVar(&cfg.TLSServerName, "tls-server-name", "", "gateway certificate hostname override (verification stays enabled)")
	flag.StringVar(&reportPath, "report", "robot-report.json", "JSON report output path")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected positional arguments")
		return 2
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	report, runErr := loadtest.Run(ctx, cfg, os.Stderr)
	if report.SchemaVersion == 0 {
		fmt.Fprintln(os.Stderr, runErr)
		return 2
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot encode report:", err)
		return 1
	}
	data = append(data, '\n')
	if _, err := os.Stdout.Write(data); err != nil {
		fmt.Fprintln(os.Stderr, "cannot write report to stdout:", err)
		return 1
	}
	if reportPath != "" {
		if err := saveReport(reportPath, data); err != nil {
			fmt.Fprintln(os.Stderr, "cannot save report:", err)
			return 1
		}
	}
	fmt.Fprintf(os.Stderr, "prepared=%d authenticated=%d messages=%d rate=%.2f/s errors=%.2f%% p50=%.3fms p95=%.3fms p99=%.3fms passed=%t\n",
		report.Prepared, report.Counters.AuthOK, report.Counters.MessagesOK, report.MessagesPerSecond, report.MessageErrorRate*100,
		report.MessageLatency.P50MS, report.MessageLatency.P95MS, report.MessageLatency.P99MS, report.Passed)
	if runErr != nil {
		fmt.Fprintln(os.Stderr, runErr)
		return 1
	}
	return 0
}

func saveReport(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".robot-report-*.json")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}
