// Package openai holds the subset of the OpenAI chat-completions wire format
// that the gateway and the simulator need. Only routing-relevant fields are
// parsed; the gateway forwards the original body bytes untouched.
package openai

import (
	"encoding/json"
	"strings"
)

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// UnmarshalJSON accepts both the string form and the array-of-parts form of
// content, flattening text parts so prefix hashing sees the same bytes
// regardless of how the client encoded the message.
func (m *Message) UnmarshalJSON(b []byte) error {
	var raw struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	m.Role = raw.Role
	if len(raw.Content) == 0 || raw.Content[0] != '[' {
		var s string
		if err := json.Unmarshal(raw.Content, &s); err != nil {
			return err
		}
		m.Content = s
		return nil
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw.Content, &parts); err != nil {
		return err
	}
	var sb strings.Builder
	for _, p := range parts {
		if p.Type == "text" {
			sb.WriteString(p.Text)
		}
	}
	m.Content = sb.String()
	return nil
}

type ChatRequest struct {
	Model       string    `json:"model"`
	Messages    []Message `json:"messages"`
	Stream      bool      `json:"stream"`
	MaxTokens   int       `json:"max_tokens"`
	Temperature float64   `json:"temperature"`
	User        string    `json:"user"`
}

// PromptText is the concatenated prompt used for token estimates and prefix
// hashing. Role prefixes are included so a system prompt and a user message
// with identical text don't collide.
func (r *ChatRequest) PromptText() string {
	var sb strings.Builder
	for _, m := range r.Messages {
		sb.WriteString(m.Role)
		sb.WriteString(": ")
		sb.WriteString(m.Content)
		sb.WriteByte('\n')
	}
	return sb.String()
}

// EstimateTokens approximates tokens as chars/4, the usual rule of thumb for
// English text. Good enough for capacity planning; no tokenizer dependency.
func EstimateTokens(s string) int {
	return (len(s) + 3) / 4
}

type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type Choice struct {
	Index        int     `json:"index"`
	Message      Message `json:"message"`
	FinishReason string  `json:"finish_reason"`
}

type ChatResponse struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []Choice `json:"choices"`
	Usage   Usage    `json:"usage"`
}

type Delta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

type ChunkChoice struct {
	Index        int     `json:"index"`
	Delta        Delta   `json:"delta"`
	FinishReason *string `json:"finish_reason"`
}

type ChatChunk struct {
	ID      string        `json:"id"`
	Object  string        `json:"object"`
	Created int64         `json:"created"`
	Model   string        `json:"model"`
	Choices []ChunkChoice `json:"choices"`
	Usage   *Usage        `json:"usage,omitempty"`
}

type ErrorResponse struct {
	Error struct {
		Message string `json:"message"`
		Type    string `json:"type"`
		Code    string `json:"code,omitempty"`
	} `json:"error"`
}

func NewError(msg, typ, code string) ErrorResponse {
	var e ErrorResponse
	e.Error.Message, e.Error.Type, e.Error.Code = msg, typ, code
	return e
}
