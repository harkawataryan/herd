// Package extproc exposes the scheduler as an Envoy External Processor, the
// same shape as the endpoint picker in Kubernetes' Gateway API Inference
// Extension: Envoy sends each request's headers and body, the processor
// answers with the replica to use, and an ORIGINAL_DST cluster keyed on that
// header carries the request there.
package extproc

import (
	"encoding/json"
	"io"
	"log"
	"net/url"
	"strings"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	typev3 "github.com/envoyproxy/go-control-plane/envoy/type/v3"

	"github.com/harkawataryan/herd/internal/backend"
	"github.com/harkawataryan/herd/internal/gateway"
	"github.com/harkawataryan/herd/internal/openai"
	"github.com/harkawataryan/herd/internal/prefix"
	"github.com/harkawataryan/herd/internal/scheduler"
)

// DestinationHeader is what Envoy's ORIGINAL_DST cluster reads.
const DestinationHeader = "x-gateway-destination-endpoint"

type Server struct {
	extprocv3.UnimplementedExternalProcessorServer
	gw *gateway.Gateway
}

func New(gw *gateway.Gateway) *Server { return &Server{gw: gw} }

// Process handles one request/response stream from Envoy.
func (s *Server) Process(stream extprocv3.ExternalProcessor_ProcessServer) error {
	ctx := stream.Context()
	var (
		apiKey, crit string
		chosen       *backend.Replica
		tokens       int
	)
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			if chosen != nil {
				chosen.End(0, false)
			}
			return err
		}
		switch v := msg.Request.(type) {
		case *extprocv3.ProcessingRequest_RequestHeaders:
			for _, h := range v.RequestHeaders.GetHeaders().GetHeaders() {
				switch strings.ToLower(h.Key) {
				case "authorization":
					apiKey = strings.TrimPrefix(string(h.RawValue), "Bearer ")
					if apiKey == "" {
						apiKey = strings.TrimPrefix(h.Value, "Bearer ")
					}
				case "x-criticality":
					crit = h.Value
					if crit == "" {
						crit = string(h.RawValue)
					}
				}
			}
			// Nothing to decide until the body arrives; ask Envoy to keep going.
			if err := stream.Send(&extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestHeaders{
				RequestHeaders: &extprocv3.HeadersResponse{}}}); err != nil {
				return err
			}

		case *extprocv3.ProcessingRequest_RequestBody:
			var req openai.ChatRequest
			_ = json.Unmarshal(v.RequestBody.Body, &req)
			prompt := req.PromptText()
			maxTokens := req.MaxTokens
			if maxTokens <= 0 {
				maxTokens = 128
			}
			sreq := scheduler.Request{Model: req.Model, Chain: prefix.Chain(prompt), PromptTokens: openai.EstimateTokens(prompt), MaxTokens: maxTokens}
			rec := s.gw.NewRecord(apiKey, req.Model, crit, sreq.PromptTokens)
			target, reason, ok := s.gw.Decide(ctx, sreq, apiKey, crit, &rec)
			if !ok {
				s.gw.Record(rec, 503, 0, 0, 0)
				return stream.Send(&extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ImmediateResponse{
					ImmediateResponse: &extprocv3.ImmediateResponse{
						Status: &typev3.HttpStatus{Code: typev3.StatusCode_ServiceUnavailable},
						Body:   []byte(`{"error":{"message":"all replicas saturated: ` + reason + `","type":"server_error","code":"saturated"}}`),
						Headers: &extprocv3.HeaderMutation{SetHeaders: []*corev3.HeaderValueOption{
							{Header: &corev3.HeaderValue{Key: "content-type", RawValue: []byte("application/json")}},
							{Header: &corev3.HeaderValue{Key: "retry-after", RawValue: []byte("2")}},
						}},
					}}})
			}
			chosen, tokens = target, sreq.PromptTokens+maxTokens
			chosen.Begin(tokens)
			s.gw.Record(rec, 0, 0, 0, 0) // provisional; completion is recorded on response headers
			host := hostPort(target.URL)
			log.Printf("extproc: %s -> %s (%s)", rec.ID, target.Name, reason)
			if err := stream.Send(&extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestBody{
				RequestBody: &extprocv3.BodyResponse{Response: &extprocv3.CommonResponse{
					HeaderMutation: &extprocv3.HeaderMutation{SetHeaders: []*corev3.HeaderValueOption{
						{Header: &corev3.HeaderValue{Key: DestinationHeader, RawValue: []byte(host)}},
						{Header: &corev3.HeaderValue{Key: "x-herd-backend", RawValue: []byte(target.Name)}},
						{Header: &corev3.HeaderValue{Key: "x-herd-reason", RawValue: []byte(reason)}},
					}},
				}}}}); err != nil {
				chosen.End(0, false)
				return err
			}

		case *extprocv3.ProcessingRequest_ResponseHeaders:
			status := 0
			for _, h := range v.ResponseHeaders.GetHeaders().GetHeaders() {
				if h.Key == ":status" {
					json.Unmarshal([]byte(h.Value+string(h.RawValue)), &status)
				}
			}
			if chosen != nil {
				chosen.End(0, status == 200)
				chosen = nil
			}
			if err := stream.Send(&extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseHeaders{
				ResponseHeaders: &extprocv3.HeadersResponse{}}}); err != nil {
				return err
			}

		default:
			// Response body/trailers are not requested in the Envoy config; be lenient.
			if err := stream.Send(&extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseBody{
				ResponseBody: &extprocv3.BodyResponse{}}}); err != nil {
				return err
			}
		}
	}
}

func hostPort(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	if u.Port() == "" {
		if u.Scheme == "https" {
			return u.Hostname() + ":443"
		}
		return u.Hostname() + ":80"
	}
	return u.Host
}
