package backend

import (
	"bufio"
	"strings"
	"testing"
	"time"
)

func TestParseMetricsVLLMAndLlamaCpp(t *testing.T) {
	text := `# HELP vllm:gpu_cache_usage_perc KV
# TYPE vllm:gpu_cache_usage_perc gauge
vllm:gpu_cache_usage_perc{model_name="x"} 0.42
vllm:num_requests_waiting{model_name="x"} 3
llamacpp:kv_cache_usage_ratio 0.9
llamacpp:requests_processing 5
junk line without value
`
	m := ParseMetrics(bufio.NewScanner(strings.NewReader(text)))
	if m["vllm:num_requests_waiting"] != 3 {
		t.Fatalf("waiting: %v", m["vllm:num_requests_waiting"])
	}
	// llama.cpp names alias onto the vLLM names the scheduler reads.
	if m["vllm:gpu_cache_usage_perc"] != 0.9 || m["vllm:num_requests_running"] != 5 {
		t.Fatalf("llama.cpp aliases not applied: %v", m)
	}
}

func TestKVEstimateIncludesTokensSinceScrape(t *testing.T) {
	r := NewReplica("a", "http://a", Local, nil)
	r.KVCapacity = 1000
	r.Begin(250)
	st := r.State()
	if st.Inflight != 1 || st.KVEstimate != 0.25 {
		t.Fatalf("expected inflight 1 and estimate 0.25, got %+v", st)
	}
	r.End(100*time.Millisecond, true)
	st = r.State()
	if st.Inflight != 0 || st.EWMATTFTMs != 100 || st.Completed != 1 {
		t.Fatalf("after End: %+v", st)
	}
}

func TestServesWithModelMap(t *testing.T) {
	r := NewReplica("cloud", "http://c", Cloud, []string{"gpt-4o-mini"})
	r.ModelMap = map[string]string{"llama-3-8b": "gpt-4o-mini"}
	if !r.Serves("llama-3-8b") || !r.Serves("gpt-4o-mini") || r.Serves("other") || !r.Serves("") {
		t.Fatal("model map should make the local model id routable")
	}
}
