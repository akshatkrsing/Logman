package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	pb "github.com/akshatkrsing/logman/gen/logmanpb"
	raftnode "github.com/akshatkrsing/logman/internal/raft"

	hashiraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

type Server struct {
	pb.UnimplementedLogServiceServer
	Node *raftnode.Node
}

func (s *Server) SendLog(ctx context.Context, req *pb.LogRequest) (*pb.LogResponse, error) {
	if req.GetMessage() == "" {
		return nil, status.Error(codes.InvalidArgument, "log message cannot be empty")
	}

	if s.Node.Raft.State() != hashiraft.Leader {
		return nil, status.Error(
			codes.FailedPrecondition,
			"log writes must be sent to the Raft leader",
		)
	}

	logID := req.GetRequestId()
	if logID == "" {
		logID = fmt.Sprintf("log-%d", time.Now().UnixNano())
	}

	timestamp := req.GetTimestamp()
	if timestamp == 0 {
		timestamp = time.Now().Unix()
	}

	entry := raftnode.LogEntry{
		ID:          logID,
		RequestID:   req.GetRequestId(),
		Message:     req.GetMessage(),
		Level:       req.GetLevel(),
		ServiceName: req.GetServiceName(),
		Timestamp:   timestamp,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to serialize log")
	}

	future := s.Node.Raft.Apply(data, 5*time.Second)

	if err := future.Error(); err != nil {
		return nil, status.Errorf(codes.Unavailable, "Raft write failed: %v", err)
	}

	if result, ok := future.Response().(error); ok {
		return nil, status.Errorf(codes.Internal, "FSM apply failed: %v", result)
	}

	log.Printf("Log committed through Raft: %s", logID)

	return &pb.LogResponse{
		Success: true,
		Message: "Log committed successfully",
		LogId:   logID,
	}, nil
}

func (s *Server) GetLogs(ctx context.Context, req *pb.GetLogsRequest) (*pb.GetLogsResponse, error) {
	entries := s.Node.FSM.GetLogs()

	limit := int(req.GetLimit())
	if limit > 0 && len(entries) > limit {
		entries = entries[len(entries)-limit:]
	}

	response := &pb.GetLogsResponse{}

	for _, entry := range entries {
		response.Logs = append(response.Logs, &pb.LogEntry{
			LogId:       entry.ID,
			RequestId:   entry.RequestID,
			Message:     entry.Message,
			Level:       entry.Level,
			ServiceName: entry.ServiceName,
			Timestamp:   entry.Timestamp,
		})
	}

	return response, nil
}

func StartGRPCServer(port string, node *raftnode.Node) (*grpc.Server, error) {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return nil, err
	}

	server := grpc.NewServer()

	pb.RegisterLogServiceServer(server, &Server{
		Node: node,
	})

	reflection.Register(server)

	log.Printf("Logman gRPC server running on port %s", port)

	go func() {
		if err := server.Serve(listener); err != nil {
			log.Printf("gRPC server stopped: %v", err)
		}
	}()

	return server, nil
}
