// Package config is the gateway's YAML configuration.
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Backend struct {
	Name       string   `yaml:"name"`
	URL        string   `yaml:"url"`
	Models     []string `yaml:"models"`
	KVCapacity int      `yaml:"kv_capacity"`
	MaxBatch   int      `yaml:"max_batch"`
}

type Provider struct {
	Name         string            `yaml:"name"`
	URL          string            `yaml:"url"`
	Models       []string          `yaml:"models"`
	ModelMap     map[string]string `yaml:"model_map"` // local model id -> provider model id
	PriceInPerM  float64           `yaml:"price_in_per_m"`
	PriceOutPerM float64           `yaml:"price_out_per_m"`
	APIKeyEnv    string            `yaml:"api_key_env"`
}

type Tenant struct {
	Name      string  `yaml:"name"`
	APIKey    string  `yaml:"api_key"`
	Weight    int     `yaml:"weight"`
	BudgetUSD float64 `yaml:"budget_usd"` // cloud spend allowed; 0 = no spillover for this tenant
}

type Config struct {
	Listen              string     `yaml:"listen"`
	Policy              string     `yaml:"policy"`
	ScrapeIntervalMs    int        `yaml:"scrape_interval_ms"`
	SaturationThreshold float64    `yaml:"saturation_threshold"`
	SpillThreshold      float64    `yaml:"spill_threshold"`
	MaxQueueWaitMs      int        `yaml:"max_queue_wait_ms"`
	DefaultMaxTokens    int        `yaml:"default_max_tokens"`
	ValueHalfLifeMs     int        `yaml:"value_half_life_ms"` // pack: block popularity halves after this much inactivity
	SessionGapMs        int        `yaml:"session_gap_ms"`     // pack: assumed gap between turns until a session shows its own
	EvictWeight         float64    `yaml:"evict_weight"`       // pack: prefill blocks one expected future hit is worth (default 1)
	PrefillTPS          float64    `yaml:"prefill_tps"`        // pack: servers' prompt-processing rate, tokens/s (default 4000)
	PackSlackBlocks     float64    `yaml:"pack_slack_blocks"`  // pack: extra cost accepted to keep a conversation or family in place (default 8)
	UIDir               string     `yaml:"ui_dir"`
	Backends            []Backend  `yaml:"backends"`
	Providers           []Provider `yaml:"providers"`
	Tenants             []Tenant   `yaml:"tenants"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	c.Defaults()
	return &c, nil
}

func (c *Config) Defaults() {
	if c.Listen == "" {
		c.Listen = ":8080"
	}
	if c.Policy == "" {
		c.Policy = "pack"
	}
	if c.ScrapeIntervalMs == 0 {
		c.ScrapeIntervalMs = 500
	}
	if c.SaturationThreshold == 0 {
		c.SaturationThreshold = 0.9
	}
	if c.SpillThreshold == 0 {
		c.SpillThreshold = 0.85
	}
	if c.MaxQueueWaitMs == 0 {
		c.MaxQueueWaitMs = 10000
	}
	if c.DefaultMaxTokens == 0 {
		c.DefaultMaxTokens = 128
	}
	if c.ValueHalfLifeMs == 0 {
		c.ValueHalfLifeMs = 60000
	}
	if c.SessionGapMs == 0 {
		c.SessionGapMs = 5000
	}
	if c.EvictWeight == 0 {
		c.EvictWeight = 1
	}
	if c.PrefillTPS == 0 {
		c.PrefillTPS = 4000
	}
	if c.PackSlackBlocks == 0 {
		c.PackSlackBlocks = 8
	}
	for i := range c.Tenants {
		if c.Tenants[i].Weight == 0 {
			c.Tenants[i].Weight = 1
		}
	}
}
