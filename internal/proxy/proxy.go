// Package proxy forwards a chat completion to a replica and streams the
// answer back, measuring what the scheduler cares about: time to first
// token, time per output token, and token counts.
package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/harkawataryan/herd/internal/openai"
)

type Result struct {
	Status           int
	TTFT             time.Duration
	Duration         time.Duration
	PromptTokens     int
	CompletionTokens int
	Err              error
}

// Client is a reusable HTTP client with streaming-friendly settings.
var Client = &http.Client{
	Timeout: 0, // streaming responses can be long; the request context bounds them
	Transport: &http.Transport{
		MaxIdleConnsPerHost: 256,
		IdleConnTimeout:     90 * time.Second,
	},
}

// Options control how the outbound request is built.
type Options struct {
	Model  string // rewritten model id for providers that use different names
	APIKey string // bearer token for providers; empty leaves the client's header alone
}

// Do forwards body to targetURL/v1/chat/completions and copies the response
// to w. For streaming responses it flushes each SSE event as it arrives.
func Do(ctx context.Context, w http.ResponseWriter, body []byte, stream bool, targetURL string, opt Options) Result {
	start := time.Now()
	res := Result{}
	if opt.Model != "" {
		body = rewriteModel(body, opt.Model)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", strings.TrimRight(targetURL, "/")+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		res.Err = err
		res.Status = 500
		return res
	}
	req.Header.Set("Content-Type", "application/json")
	if opt.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+opt.APIKey)
	}
	if stream {
		req.Header.Set("Accept", "text/event-stream")
	}
	resp, err := Client.Do(req)
	if err != nil {
		res.Err = err
		res.Status = 502
		return res
	}
	defer resp.Body.Close()
	res.Status = resp.StatusCode

	for _, h := range []string{"Content-Type", "X-Sim-Cached-Blocks", "X-Sim-Queue-Wait-Ms"} {
		if v := resp.Header.Get(h); v != "" {
			w.Header().Set(h, v)
		}
	}
	w.WriteHeader(resp.StatusCode)

	if !stream || resp.StatusCode != 200 {
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			res.Err = err
			return res
		}
		res.TTFT = time.Since(start)
		var full openai.ChatResponse
		if json.Unmarshal(data, &full) == nil {
			res.PromptTokens, res.CompletionTokens = full.Usage.PromptTokens, full.Usage.CompletionTokens
		}
		w.Write(data)
		res.Duration = time.Since(start)
		return res
	}

	flusher, _ := w.(http.Flusher)
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	first := true
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "data: ") && line != "data: [DONE]" {
			var chunk openai.ChatChunk
			if json.Unmarshal([]byte(line[6:]), &chunk) == nil {
				if len(chunk.Choices) > 0 && chunk.Choices[0].Delta.Content != "" {
					if first {
						res.TTFT = time.Since(start)
						first = false
					}
					res.CompletionTokens++
				}
				if chunk.Usage != nil {
					res.PromptTokens = chunk.Usage.PromptTokens
					if chunk.Usage.CompletionTokens > 0 {
						res.CompletionTokens = chunk.Usage.CompletionTokens
					}
				}
			}
		}
		w.Write([]byte(line))
		w.Write([]byte("\n"))
		if line == "" && flusher != nil {
			flusher.Flush()
		}
	}
	if flusher != nil {
		flusher.Flush()
	}
	res.Err = sc.Err()
	res.Duration = time.Since(start)
	return res
}

// rewriteModel swaps the model field without touching anything else in the
// client's JSON, so unknown fields pass through to the provider.
func rewriteModel(body []byte, model string) []byte {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return body
	}
	b, _ := json.Marshal(model)
	m["model"] = b
	out, err := json.Marshal(m)
	if err != nil {
		return body
	}
	return out
}
