package sim

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/harkawataryan/herd/internal/openai"
)

// Server exposes an Engine over the OpenAI chat API plus vLLM-style metrics.
type Server struct {
	engine *Engine
	ids    atomic.Int64
}

func NewServer(e *Engine) *Server { return &Server{engine: e} }

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", s.chat)
	mux.HandleFunc("GET /v1/models", s.models)
	mux.HandleFunc("GET /metrics", s.metrics)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte(`{"ok":true}`)) })
	return mux
}

func (s *Server) models(w http.ResponseWriter, _ *http.Request) {
	type m struct {
		ID     string `json:"id"`
		Object string `json:"object"`
	}
	var out struct {
		Object string `json:"object"`
		Data   []m    `json:"data"`
	}
	out.Object = "list"
	for _, id := range s.engine.cfg.Models {
		out.Data = append(out.Data, m{ID: id, Object: "model"})
	}
	writeJSON(w, 200, out)
}

func (s *Server) serves(model string) bool {
	if len(s.engine.cfg.Models) == 0 {
		return true
	}
	for _, m := range s.engine.cfg.Models {
		if m == model {
			return true
		}
	}
	return false
}

func (s *Server) chat(w http.ResponseWriter, r *http.Request) {
	var req openai.ChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, 400, openai.NewError("invalid JSON: "+err.Error(), "invalid_request_error", ""))
		return
	}
	if !s.serves(req.Model) {
		writeJSON(w, 404, openai.NewError("model not found: "+req.Model, "invalid_request_error", "model_not_found"))
		return
	}
	id := fmt.Sprintf("chatcmpl-%s-%d", s.engine.cfg.Name, s.ids.Add(1))
	created := time.Now().Unix()
	sreq := Request{Prompt: req.PromptText(), MaxTokens: req.MaxTokens}

	if !req.Stream {
		var sb strings.Builder
		res, err := s.engine.Generate(r.Context(), sreq, func(t string) { sb.WriteString(t) })
		if err != nil {
			return // client went away
		}
		w.Header().Set("X-Sim-Cached-Blocks", fmt.Sprint(res.CachedBlocks))
		w.Header().Set("X-Sim-Queue-Wait-Ms", fmt.Sprint(res.QueueWait.Milliseconds()))
		writeJSON(w, 200, openai.ChatResponse{
			ID: id, Object: "chat.completion", Created: created, Model: req.Model,
			Choices: []openai.Choice{{Message: openai.Message{Role: "assistant", Content: sb.String()}, FinishReason: "stop"}},
			Usage:   openai.Usage{PromptTokens: res.PromptTokens, CompletionTokens: res.CompletionTokens, TotalTokens: res.PromptTokens + res.CompletionTokens},
		})
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	flusher, _ := w.(http.Flusher)
	emit := func(t string) {
		b, _ := json.Marshal(openai.ChatChunk{ID: id, Object: "chat.completion.chunk", Created: created, Model: req.Model,
			Choices: []openai.ChunkChoice{{Delta: openai.Delta{Content: t}}}})
		fmt.Fprintf(w, "data: %s\n\n", b)
		if flusher != nil {
			flusher.Flush()
		}
	}
	res, err := s.engine.Generate(r.Context(), sreq, emit)
	if err != nil {
		return
	}
	stop := "stop"
	b, _ := json.Marshal(openai.ChatChunk{ID: id, Object: "chat.completion.chunk", Created: created, Model: req.Model,
		Choices: []openai.ChunkChoice{{Delta: openai.Delta{}, FinishReason: &stop}},
		Usage:   &openai.Usage{PromptTokens: res.PromptTokens, CompletionTokens: res.CompletionTokens, TotalTokens: res.PromptTokens + res.CompletionTokens}})
	fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", b)
	if flusher != nil {
		flusher.Flush()
	}
}

// metrics mirrors the vLLM metric names the gateway's scraper understands.
func (s *Server) metrics(w http.ResponseWriter, _ *http.Request) {
	st := s.engine.Stats()
	name := s.engine.cfg.Name
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# HELP vllm:gpu_cache_usage_perc KV cache utilization (0-1)\n# TYPE vllm:gpu_cache_usage_perc gauge\n")
	fmt.Fprintf(w, "vllm:gpu_cache_usage_perc{model_name=%q} %.4f\n", name, st.KVUtil)
	fmt.Fprintf(w, "# TYPE vllm:num_requests_running gauge\nvllm:num_requests_running{model_name=%q} %d\n", name, st.Running)
	fmt.Fprintf(w, "# TYPE vllm:num_requests_waiting gauge\nvllm:num_requests_waiting{model_name=%q} %d\n", name, st.Waiting)
	fmt.Fprintf(w, "# TYPE vllm:prefix_cache_queries_total counter\nvllm:prefix_cache_queries_total{model_name=%q} %d\n", name, st.CacheQueries)
	fmt.Fprintf(w, "# TYPE vllm:prefix_cache_hits_total counter\nvllm:prefix_cache_hits_total{model_name=%q} %d\n", name, st.CacheHits)
	fmt.Fprintf(w, "# TYPE sim_requests_total counter\nsim_requests_total{model_name=%q} %d\n", name, st.Requests)
	fmt.Fprintf(w, "# TYPE sim_prefix_cache_blocks gauge\nsim_prefix_cache_blocks{model_name=%q} %d\n", name, st.PrefixCacheInUse)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
