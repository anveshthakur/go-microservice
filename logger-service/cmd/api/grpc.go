package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/anveshthakur/log-service/data"
	"github.com/anveshthakur/log-service/logs"
	"google.golang.org/grpc"
)

type LogServer struct {
	logs.UnimplementedLogServiceServer
	Models data.Models
}

func (l *LogServer) WriteLog(ctx context.Context, req *logs.LogRequest) (*logs.LogResponse, error) {
	input := req.GetLogEntry()

	logEntry := data.LogEntry{
		Name: input.Name,
		Data: input.Data,
	}

	if err := l.Models.LogEntry.Insert(logEntry); err != nil {
		res := &logs.LogResponse{
			Result: "failed",
		}
		return res, err
	}

	return &logs.LogResponse{
		Result: "logged!",
	}, nil
}

func (app *Config) grpcListen() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", gRpcPort))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	logs.RegisterLogServiceServer(s, &LogServer{Models: app.Models})
	log.Printf("grpc server started on port %s", gRpcPort)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
