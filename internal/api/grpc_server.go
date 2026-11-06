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

func (s *Server) SendLog(
	ctx context.Context,
	req *pb.LogRequest,
) (*pb.LogResponse, error) {

	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}

	if strings.TrimSpace(req.GetMessage()) == "" {
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
		Metadata:    req.GetMetadata(),
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

func (s *Server) GetLogs(
	ctx context.Context,
	req *pb.GetLogsRequest,
) (*pb.GetLogsResponse, error) {

	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}

	// Validate timestamp range.
	if req.GetStartTimestamp() > 0 &&
		req.GetEndTimestamp() > 0 &&
		req.GetStartTimestamp() > req.GetEndTimestamp() {

		return nil, status.Error(
			codes.InvalidArgument,
			"start_timestamp cannot be greater than end_timestamp",
		)
	}

	// Validate page number.
	pageNumber := req.GetPageNumber()

	if pageNumber < 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"page_number cannot be negative",
		)
	}

	if pageNumber == 0 {
		pageNumber = 1
	}

	// Resolve page size.
	pageSize := int(req.GetPageSize())

	// Backward compatibility with the original limit field.
	if pageSize == 0 && req.GetLimit() > 0 {
		pageSize = int(req.GetLimit())
	}

	if pageSize == 0 {
		pageSize = defaultPageSize
	}

	if pageSize < 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"page_size cannot be negative",
		)
	}

	if pageSize > maxPageSize {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"page_size cannot exceed %d",
			maxPageSize,
		)
	}

	// Use FSM indexes for exact-match filters.
	indexedEntries := s.Node.FSM.SearchLogs(raftnode.LogQuery{
		RequestID:   req.GetRequestId(),
		Level:       req.GetLevel(),
		Levels:      req.GetLevels(),
		ServiceName: req.GetServiceName(),
		Metadata:    req.GetMetadata(),
	})

	filteredEntries := make([]raftnode.LogEntry, 0, len(indexedEntries))

	keywordFilter := strings.TrimSpace(req.GetKeyword())

	// Apply keyword and timestamp filters to indexed candidates.
	for _, entry := range indexedEntries {

		// Case-insensitive keyword search.
		if keywordFilter != "" &&
			!strings.Contains(
				strings.ToLower(entry.Message),
				strings.ToLower(keywordFilter),
			) {
			continue
		}

		// Inclusive start timestamp.
		if req.GetStartTimestamp() > 0 &&
			entry.Timestamp < req.GetStartTimestamp() {
			continue
		}

		// Inclusive end timestamp.
		if req.GetEndTimestamp() > 0 &&
			entry.Timestamp > req.GetEndTimestamp() {
			continue
		}

		filteredEntries = append(filteredEntries, entry)
	}

	totalCount := len(filteredEntries)

	// Calculate pagination boundaries using int64 to avoid
	// integer overflow when handling large page numbers.
	startIndex := int64(pageNumber-1) * int64(pageSize)

	if startIndex >= int64(totalCount) {
		startIndex = int64(totalCount)
	}

	endIndex := startIndex + int64(pageSize)

	if endIndex > int64(totalCount) {
		endIndex = int64(totalCount)
	}

	pageEntries := filteredEntries[int(startIndex):int(endIndex)]

	// Build response.
	response := &pb.GetLogsResponse{
		Logs:       make([]*pb.LogEntry, 0, len(pageEntries)),
		TotalCount: int32(totalCount),
		PageNumber: pageNumber,
		PageSize:   int32(pageSize),
		HasMore:    endIndex < int64(totalCount),
	}

	for _, entry := range pageEntries {
		response.Logs = append(response.Logs, &pb.LogEntry{
			LogId:       entry.ID,
			RequestId:   entry.RequestID,
			Message:     entry.Message,
			Level:       entry.Level,
			ServiceName: entry.ServiceName,
			Timestamp:   entry.Timestamp,
			Metadata:    entry.Metadata,
		})
	}

	return response, nil
}

func StartGRPCServer(
	port string,
	node *raftnode.Node,
) (*grpc.Server, error) {

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

func (s *Server) BatchSendLogs(
	ctx context.Context,
	req *pb.BatchLogRequest,
) (*pb.BatchLogResponse, error) {

	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request cannot be nil")
	}

	const maxBatchSize = 1000

	logs := req.GetLogs()

	if len(logs) == 0 {
		return nil, status.Error(codes.InvalidArgument, "batch cannot be empty")
	}

	if len(logs) > maxBatchSize {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"batch size exceeds maximum allowed size of %d",
			maxBatchSize,
		)
	}

	response := &pb.BatchLogResponse{
		Results: make([]*pb.LogResponse, 0, len(logs)),
	}

	for _, logRequest := range logs {
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}

		result, err := s.SendLog(ctx, logRequest)

		if err != nil {
			response.Results = append(response.Results, &pb.LogResponse{
				Success: false,
				Message: err.Error(),
			})
			response.RejectedCount++
			continue
		}

		response.Results = append(response.Results, result)

		if result.GetSuccess() {
			response.AcceptedCount++
		} else {
			response.RejectedCount++
		}
	}

	return response, nil
}
