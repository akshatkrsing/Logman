
package api

import (
	"context"
	"fmt"
	"log"
	"net"

	pb "github.com/akshatkrsing/logman/gen/logmanpb"
	"google.golang.org/grpc"
)

type Server struct {
	pb.UnimplementedLogServiceServer
}

func (s *Server) SendLog(ctx context.Context, req *pb.LogRequest) (*pb.LogResponse, error) {
	if req.GetMessage() == "" {
		return nil, fmt.Errorf("log message cannot be empty")
	}

	log.Printf(
		"Received log | Service: %s | Level: %s | Message: %s",
		req.GetServiceName(),
		req.GetLevel(),
		req.GetMessage(),
	)

	return &pb.LogResponse{
		Success: true,
		Message: "Log received successfully",
		LogId:   req.GetRequestId(),
	}, nil
}

func (s *Server) GetLogs(ctx context.Context, req *pb.GetLogsRequest) (*pb.GetLogsResponse, error) {
	return &pb.GetLogsResponse{}, nil
}

func StartGRPCServer(port string) error {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}

	server := grpc.NewServer()

	pb.RegisterLogServiceServer(server, &Server{})

	log.Printf("Logman gRPC server running on port %s", port)

	return server.Serve(listener)
}
