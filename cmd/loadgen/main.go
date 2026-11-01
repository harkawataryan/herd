// Command loadgen replays a deterministic workload against a Herd gateway
// and reports time-to-first-token percentiles, throughput, shedding and
// routing shares. Run it once per policy on the same seed to compare them.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/harkawataryan/herd/internal/openai"
	"github.com/harkawataryan/herd/internal/workload"
)

func main() {
	p := workload.DefaultParams()
	url := flag.String("url", "http://127.0.0.1:8080", "gateway base URL")
	flag.Float64Var(&p.RPS, "rps", p.RPS, "open-loop arrival rate in simulated requests/second")
	flag.DurationVar(&p.Duration, "duration", p.Duration, "simulated trace length")
	flag.Int64Var(&p.Seed, "seed", p.Seed, "workload seed")
	flag.StringVar(&p.Model, "model", p.Model, "model id to request")
	flag.IntVar(&p.SystemPrompts, "system-prompts", p.SystemPrompts, "distinct system prompts in the pool")
	flag.IntVar(&p.SystemPromptChars, "system-chars", p.SystemPromptChars, "mean system prompt size in chars")
	flag.Float64Var(&p.Zipf, "zipf", p.Zipf, "system prompt popularity skew (>1)")
	mix := flag.String("mix", "short=0.4,rag=0.4,long=0.2", "request class fractions")
	crit := flag.String("criticality", "standard=0.7,sheddable=0.2,critical=0.1", "criticality fractions")
	timeScale := flag.Float64("time-scale", 1, "scale arrival intervals (match the sims' -time-scale)")
	concurrency := flag.Int("concurrency", 0, "closed-loop mode: N workers back to back, ignoring arrival times")
	label := flag.String("policy-label", "", "policy name recorded in the report")
	out := flag.String("out", "", "write the JSON report here")
	samplesOut := flag.String("samples", "", "write per-request samples here (JSON lines)")
	record := flag.String("record", "", "write the generated trace to this file")
	replay := flag.String("replay", "", "replay a previously recorded trace instead of generating")
	flag.Parse()

	if m, err := parseFractions(*mix); err == nil && len(m) > 0 {
		p.Mix = map[workload.Class]float64{}
		for k, v := range m {
			p.Mix[workload.Class(k)] = v
		}
	}
	if c, err := parseFractions(*crit); err == nil && len(c) > 0 {
		p.Criticality = c
	}

	var trace *workload.Trace
	if *replay != "" {
		t, err := workload.Load(*replay)
		if err != nil {
			log.Fatalf("replay: %v", err)
		}
		trace = t
	} else {
		trace = workload.Generate(p)
		if *record != "" {
			if err := trace.Save(*record); err != nil {
				log.Fatalf("record: %v", err)
			}
		}
	}
	sum := trace.Summarize()
	log.Printf("trace: %d requests over %s sim time, classes=%v crit=%v mean prompt tokens=%.0f top prompt share=%.0f%%",
		sum.Requests, trace.Params.Duration, sum.ByClass, sum.ByCrit, sum.MeanPromptTk, 100*sum.ZipfShare())

	samples := fire(*url, trace, *timeScale, *concurrency)
	wall := time.Duration(float64(trace.Params.Duration) * *timeScale)
	report := summarize(*label, samples, wall, trace.Params.Duration, *timeScale)

	fmt.Println(report.MarkdownHeader())
	fmt.Println(report.MarkdownRow())
	log.Printf("backend share: %v", report.ByBackend)
	log.Printf("shed by criticality: %v", report.ShedByCrit)
	log.Printf("p99 TTFT by class (ms): %v", report.TTFTByClassMs)

	if *out != "" {
		b, _ := json.MarshalIndent(report, "", "  ")
		if err := os.WriteFile(*out, b, 0o644); err != nil {
			log.Fatalf("write report: %v", err)
		}
	}
	if *samplesOut != "" {
		f, err := os.Create(*samplesOut)
		if err != nil {
			log.Fatalf("write samples: %v", err)
		}
		enc := json.NewEncoder(f)
		for _, s := range samples {
			enc.Encode(s)
		}
		f.Close()
	}
}

// fire sends the trace. Open-loop mode launches each request at its
// scheduled (scaled) offset regardless of outstanding work, which is what
// exposes saturation; closed-loop mode is for throughput ceilings.
func fire(url string, trace *workload.Trace, timeScale float64, concurrency int) []Sample {
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 1024, MaxConnsPerHost: 0}}
	samples := make([]Sample, len(trace.Requests))
	var wg sync.WaitGroup

	if concurrency > 0 {
		jobs := make(chan int)
		for w := 0; w < concurrency; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range jobs {
					samples[i] = send(client, url, trace.Requests[i])
				}
			}()
		}
		for i := range trace.Requests {
			jobs <- i
		}
		close(jobs)
		wg.Wait()
		return samples
	}

	start := time.Now()
	for i, r := range trace.Requests {
		at := time.Duration(float64(r.At) * timeScale)
		if d := time.Until(start.Add(at)); d > 0 {
			time.Sleep(d)
		}
		wg.Add(1)
		go func(i int, r workload.Request) {
			defer wg.Done()
			samples[i] = send(client, url, r)
		}(i, r)
	}
	wg.Wait()
	return samples
}

func send(client *http.Client, url string, r workload.Request) Sample {
	s := Sample{Seq: r.Seq, Tenant: r.Tenant, Class: string(r.Class), Criticality: r.Criticality}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", url+"/v1/chat/completions", bytes.NewReader(r.Body()))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+r.APIKey)
	req.Header.Set("X-Criticality", r.Criticality)
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		s.Err = err.Error()
		return s
	}
	defer resp.Body.Close()
	s.Status = resp.StatusCode
	s.Backend = resp.Header.Get("X-Herd-Backend")
	s.Reason = resp.Header.Get("X-Herd-Reason")
	if resp.StatusCode != 200 {
		s.Total = time.Since(start)
		return s
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") || line == "data: [DONE]" {
			continue
		}
		var chunk openai.ChatChunk
		if json.Unmarshal([]byte(line[6:]), &chunk) != nil {
			continue
		}
		if len(chunk.Choices) > 0 && chunk.Choices[0].Delta.Content != "" {
			if s.TTFT == 0 {
				s.TTFT = time.Since(start)
			}
			s.Tokens++
		}
		if chunk.Usage != nil && chunk.Usage.CompletionTokens > 0 {
			s.Tokens = chunk.Usage.CompletionTokens
		}
	}
	if err := sc.Err(); err != nil {
		s.Err = err.Error()
	}
	s.Total = time.Since(start)
	return s
}

func parseFractions(s string) (map[string]float64, error) {
	out := map[string]float64{}
	for _, part := range strings.Split(s, ",") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("bad fraction %q", part)
		}
		var v float64
		if _, err := fmt.Sscanf(kv[1], "%g", &v); err != nil {
			return nil, err
		}
		out[kv[0]] = v
	}
	return out, nil
}
