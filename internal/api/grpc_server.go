package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	pb "github.com/akshatkrsing/logman/gen/logmanpb"
	raftnode "github.com/akshatkrsing/logman/internal/raft"

	hashiraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

const (
	defaultPageSize = 50
	maxPageSize     = 500
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
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}

	if req.GetStartTimestamp() > 0 &&
		req.GetEndTimestamp() > 0 &&
		req.GetStartTimestamp() > req.GetEndTimestamp() {
		return nil, status.Error(
			codes.InvalidArgument,
			"start_timestamp cannot be greater than end_timestamp",
		)
	}

	pageNumber := int(req.GetPageNumber())
	if pageNumber < 0 {
		return nil, status.Error(codes.InvalidArgument, "page_number cannot be negative")
	}
	if pageNumber == 0 {
		pageNumber = 1
	}

	pageSize := int(req.GetPageSize())

	// Backward compatibility with the old limit field.
	if pageSize == 0 && req.GetLimit() > 0 {
		pageSize = int(req.GetLimit())
	}

	if pageSize == 0 {
		pageSize = defaultPageSize
	}

	if pageSize < 0 {
		return nil, status.Error(codes.InvalidArgument, "page_size cannot be negative")
	}

	if pageSize > maxPageSize {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"page_size cannot exceed %d",
			maxPageSize,
		)
	}

	allEntries := s.Node.FSM.GetLogs()
	filteredEntries := make([]raftnode.LogEntry, 0, len(allEntries))

	levelFilter := strings.TrimSpace(req.GetLevel())
	serviceFilter := strings.TrimSpace(req.GetServiceName())

	// Apply all filters before pagination.
	for _, entry := range allEntries {
		if levelFilter != "" &&
			!strings.EqualFold(entry.Level, levelFilter) {
			continue
		}

		if serviceFilter != "" &&
			!strings.EqualFold(entry.ServiceName, serviceFilter) {
			continue
		}

		if req.GetStartTimestamp() > 0 &&
			entry.Timestamp < req.GetStartTimestamp() {
			continue
		}

		if req.GetEndTimestamp() > 0 &&
			entry.Timestamp > req.GetEndTimestamp() {
			continue
		}

		filteredEntries = append(filteredEntries, entry)
	}

	totalCount := len(filteredEntries)

	// Calculate pagination boundaries safely.
	startIndex := 0
	if pageNumber > 1 {
		if pageNumber-1 > totalCount/pageSize {
			startIndex = totalCount
		} else {
			startIndex = (pageNumber - 1) * pageSize
			if startIndex > totalCount {
				startIndex = totalCount
			}
		}
	}

	endIndex := startIndex + pageSize
	if endIndex > totalCount {
		endIndex = totalCount
	}

	pageEntries := filteredEntries[startIndex:endIndex]

	response := &pb.GetLogsResponse{
		Logs:       make([]*pb.LogEntry, 0, len(pageEntries)),
		TotalCount: int32(totalCount),
		PageNumber: int32(pageNumber),
		PageSize:   int32(pageSize),
		HasMore:    endIndex < totalCount,
	}

	for _, entry := range pageEntries {
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
