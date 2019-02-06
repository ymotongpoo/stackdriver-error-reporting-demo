// Copyright 2019 Yoshi Yamaguchi
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"contrib.go.opencensus.io/exporter/stackdriver"
	pb "github.com/ymotongpoo/stackdriver-error-reporting-demo/src/tokenize/genproto"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	listenPort       = 5050
	projectID        = "yoshifumi-cloud-demo"
	traceLogFieldKey = "logging.googleapis.com/trace"
	spanLogFieldKey  = "logging.googleapis.com/span"

	grpcTimeout = 3 * time.Second
)

var (
	logger   *zap.SugaredLogger
	exporter *stackdriver.Exporter

	countSvcAddr string = "countservice:5051"
)

func main() {
	var err error
	ctx := context.Background()

	logger, err = initLogger()
	if err != nil {
		log.Fatalf("failed to initialize logger: %v", err)
	}
	go initTraceAndStats()

	ts, err := NewTokenizeServiceServer(ctx, countSvcAddr)
	if err != nil {
		logger.Fatalf("failed to create gRPC server: %v", err)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%v", listenPort))
	if err != nil {
		logger.Fatalf("failed to listen port %v: %v", listenPort, err)
	}
	srv := grpc.NewServer(grpc.StatsHandler(&ocgrpc.ServerHandler{}))
	pb.RegisterTokenizeServiceServer(srv, ts)
	err = srv.Serve(lis)
	logger.Fatal(err)
}

// gRPC

type TokenizeServiceServer struct {
	client pb.CountServiceClient
}

func NewTokenizeServiceServer(ctx context.Context, countSvcAddr string) (*TokenizeServiceServer, error) {
	countSvcConn, err := grpc.DialContext(ctx, countSvcAddr,
		grpc.WithInsecure(),
		grpc.WithTimeout(grpcTimeout),
		grpc.WithStatsHandler(&ocgrpc.ClientHandler{}))
	if err != nil {
		return nil, err
	}
	client := pb.NewCountServiceClient(countSvcConn)
	return &TokenizeServiceServer{
		client: client,
	}, nil
}

func (t *TokenizeServiceServer) Tokenize(ctx context.Context, tr *pb.TokenizeRequest) (*pb.ModalResponse, error) {
	span := trace.FromContext(ctx)
	sc := span.SpanContext()
	l := logger.With(
		traceLogFieldKey, sc.TraceID.String(),
		spanLogFieldKey, sc.SpanID.String(),
	)

	l.Infof("[tokenize] start tokenize")

	// NOTE: very simple
	src := tr.GetSourceText()
	words := strings.Split(src, " ")
	c := &pb.CountRequest{
		Words: words,
	}
	mr, err := t.client.CountWords(ctx, c)
	if err != nil {
		l.Errorf("[tokenize] error tokenizing the words: %v", src)
		return nil, err
	}

	l.Infof("[tokenize] end tokenize")

	return mr, nil
}

func (t *TokenizeServiceServer) Check(ctx context.Context, r *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{
		Status: healthpb.HealthCheckResponse_SERVING,
	}, nil
}

func (t *TokenizeServiceServer) Watch(r *healthpb.HealthCheckRequest, _ healthpb.Health_WatchServer) error {
	return nil
}

// initialization

func initLogger() (*zap.SugaredLogger, error) {
	cfg := zap.Config{
		Encoding:         "json",
		Level:            zap.NewAtomicLevelAt(zapcore.DebugLevel),
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey:     "message",
			LevelKey:       "severity",
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			TimeKey:        "timestamp",
			EncodeTime:     zapcore.ISO8601TimeEncoder,
			EncodeDuration: zapcore.StringDurationEncoder,
			CallerKey:      "caller",
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
	}

	l, err := cfg.Build()
	if err != nil {
		return nil, err
	}
	return l.Sugar(), nil
}

func initTraceAndStats() {
	// init Trace with OpenCensus
	// NOTE: trace.AlwaysSample() is for demo purpose
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	var err error
	exporter, err = stackdriver.NewExporter(stackdriver.Options{
		ProjectID:    projectID,
		MetricPrefix: "tokenier",
	})
	if err != nil {
		log.Fatalf("failed to create OC exporter: %v", err)
	}
	defer exporter.Flush()
	trace.RegisterExporter(exporter)

	// init Stats with OpenCensus
	view.SetReportingPeriod(60 * time.Second)
	view.RegisterExporter(exporter)
	if err := view.Register(ocgrpc.DefaultServerViews...); err != nil {
		log.Fatalf("failed to register server views: %v", err)
	}
	if err := view.Register(ocgrpc.DefaultClientViews...); err != nil {
		log.Fatalf("failed to register client views: %v", err)
	}
}
