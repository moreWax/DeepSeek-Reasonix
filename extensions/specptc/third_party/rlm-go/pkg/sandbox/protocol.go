package sandbox

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/XiaoConstantine/rlm-go/pkg/core"
)

const (
	maxIPCExecutionMessages = 512
	maxIPCExecutionBytes    = 16 << 20
	maxIPCConnections       = 16
	maxIPCReservations      = 64
	maxIPCCallRecords       = 1024
	maxRecordedTextBytes    = 4096
)

// MessageType identifies the type of IPC message.
type MessageType string

const (
	// MessageQuery is a Query() call from the sandbox to the host.
	MessageQuery MessageType = "query"

	// MessageQueryBatched is a QueryBatched() call from the sandbox.
	MessageQueryBatched MessageType = "query_batched"

	// MessageQueryBlocked records a sandbox-side query guardrail result.
	MessageQueryBlocked MessageType = "query_blocked"

	// MessageReserveQuery reserves one source-ordered async query claim.
	MessageReserveQuery MessageType = "reserve_query"

	// MessageResolveQuery resolves a previously reserved async query.
	MessageResolveQuery MessageType = "resolve_query"

	// MessageResponse is a response from the host to the sandbox.
	MessageResponse MessageType = "response"

	// MessageError is an error response from the host.
	MessageError MessageType = "error"

	// MessageReady indicates the sandbox is ready.
	MessageReady MessageType = "ready"

	// MessageExit indicates the sandbox is exiting.
	MessageExit MessageType = "exit"
)

// IPCMessage is the JSON message format for host-sandbox communication.
type IPCMessage struct {
	// Type identifies the message type.
	Type MessageType `json:"type"`

	// ID is a unique identifier for request-response correlation.
	ID string `json:"id,omitempty"`

	// ExecutionID identifies the container execution that produced this message.
	ExecutionID uint64 `json:"execution_id,omitempty"`

	// Prompt is the query prompt (for Query messages).
	Prompt string `json:"prompt,omitempty"`

	// Prompts is a list of query prompts (for QueryBatched messages).
	Prompts []string `json:"prompts,omitempty"`

	// Response is the LLM response (for Response messages).
	Response string `json:"response,omitempty"`

	// Responses is a list of LLM responses (for batched Response messages).
	Responses []string `json:"responses,omitempty"`

	// Error is the error message (for Error messages).
	Error string `json:"error,omitempty"`

	// TokenUsage contains token usage metadata.
	TokenUsage *TokenUsage `json:"token_usage,omitempty"`

	// TokenUsages contains token usage for batched queries.
	TokenUsages []TokenUsage `json:"token_usages,omitempty"`

	// Duration is the execution time in seconds.
	Duration float64 `json:"duration,omitempty"`

	// ReservationID identifies a source-ordered asynchronous query claim.
	ReservationID string `json:"reservation_id,omitempty"`
}

// TokenUsage tracks token usage for an LLM call.
type TokenUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
}

type reservedIPCQuery struct {
	executionID uint64
	prompt      string
	query       core.QueryReservation
}

// IPCServer handles incoming IPC requests from the sandbox container.
type IPCServer struct {
	listener     net.Listener
	client       LLMClient
	port         int
	mu           sync.RWMutex
	calls        []LLMCall
	running      bool
	ctx          context.Context
	cancel       context.CancelFunc
	execCtx      context.Context
	execID       uint64
	execMessages int
	execBytes    int64
	reservation  uint64
	reserved     map[string]reservedIPCQuery
	connSem      chan struct{}
	socketDir    string
	socketPath   string
}

// NewIPCServer creates a new IPC server that handles Query() calls from the sandbox.
func NewIPCServer(client LLMClient, port int) (*IPCServer, error) {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	if port == 0 {
		addr = "127.0.0.1:0"
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to start IPC server: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Get the assigned port if auto-assigned
	actualPort := listener.Addr().(*net.TCPAddr).Port

	return &IPCServer{
		listener: listener,
		client:   client,
		port:     actualPort,
		calls:    nil,
		running:  false,
		ctx:      ctx,
		cancel:   cancel,
		reserved: make(map[string]reservedIPCQuery),
		connSem:  make(chan struct{}, maxIPCConnections),
	}, nil
}

// NewUnixIPCServer creates a filesystem-scoped IPC server suitable for
// bind-mounting into a container running with --network none.
func NewUnixIPCServer(client LLMClient) (*IPCServer, error) {
	base := "/tmp"
	if _, err := os.Stat(base); err != nil {
		base = os.TempDir()
	}
	dir, err := os.MkdirTemp(base, "rxi-*")
	if err != nil {
		return nil, fmt.Errorf("create IPC directory: %w", err)
	}
	path := filepath.Join(dir, "query.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("start Unix IPC server: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = listener.Close()
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("restrict Unix IPC socket: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &IPCServer{
		listener: listener, client: client, ctx: ctx, cancel: cancel,
		reserved:  make(map[string]reservedIPCQuery),
		connSem:   make(chan struct{}, maxIPCConnections),
		socketDir: dir, socketPath: path,
	}, nil
}

// SocketDir returns the host directory containing the Unix socket.
func (s *IPCServer) SocketDir() string { return s.socketDir }

// SocketPath returns the host Unix socket path.
func (s *IPCServer) SocketPath() string { return s.socketPath }

// Port returns the port the server is listening on.
func (s *IPCServer) Port() int {
	return s.port
}

// Address returns the full address (host:port) the server is listening on.
func (s *IPCServer) Address() string {
	return s.listener.Addr().String()
}

// Start begins accepting connections.
func (s *IPCServer) Start() {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.running = true
	s.mu.Unlock()

	go s.acceptLoop()
}

// acceptLoop accepts and handles incoming connections.
func (s *IPCServer) acceptLoop() {
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		// Set a deadline to periodically check for cancellation
		if tcpListener, ok := s.listener.(*net.TCPListener); ok {
			_ = tcpListener.SetDeadline(time.Now().Add(1 * time.Second))
		}

		conn, err := s.listener.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue // Timeout, check for cancellation
			}
			// Check if we're shutting down
			select {
			case <-s.ctx.Done():
				return
			default:
			}
			continue
		}

		select {
		case s.connSem <- struct{}{}:
			go func() {
				defer func() { <-s.connSem }()
				s.handleConnection(conn)
			}()
		case <-s.ctx.Done():
			_ = conn.Close()
			return
		}
	}
}

const MaxIPCFrameBytes = 2 << 20

func readIPCFrame(reader *bufio.Reader) ([]byte, error) {
	frame := make([]byte, 0, 4096)
	for {
		fragment, err := reader.ReadSlice('\n')
		if len(frame)+len(fragment) > MaxIPCFrameBytes {
			return nil, fmt.Errorf("IPC frame exceeds %d bytes", MaxIPCFrameBytes)
		}
		frame = append(frame, fragment...)
		if err == nil {
			return frame, nil
		}
		if err != bufio.ErrBufferFull {
			return nil, err
		}
	}
}

func boundedIPCResponse(response IPCMessage) IPCMessage {
	encoded, err := json.Marshal(response)
	if err == nil && len(encoded)+1 <= MaxIPCFrameBytes {
		return response
	}
	return IPCMessage{
		Type: MessageError, ID: response.ID,
		Error: fmt.Sprintf("IPC response exceeds %d bytes", MaxIPCFrameBytes),
	}
}

// handleConnection processes messages from a single connection.
func (s *IPCServer) handleConnection(conn net.Conn) {
	defer func() { _ = conn.Close() }()

	reader := bufio.NewReader(conn)
	encoder := json.NewEncoder(conn)

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		// Set read deadline
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Minute))

		// Read one size-bounded JSON message per line.
		line, err := readIPCFrame(reader)
		if err != nil {
			if err == io.EOF {
				return
			}
			return
		}

		// Parse the message
		var msg IPCMessage
		if err := json.Unmarshal(line, &msg); err != nil {
			s.sendError(encoder, "", fmt.Sprintf("invalid JSON: %v", err))
			continue
		}
		if err := s.admitIPCMessage(msg, len(line)); err != nil {
			s.sendError(encoder, msg.ID, err.Error())
			return
		}

		// Handle and size-bound the response before it crosses into the worker.
		response := boundedIPCResponse(s.handleMessage(msg))
		if err := encoder.Encode(response); err != nil {
			return // Connection error
		}

		// Check for exit message
		if msg.Type == MessageExit {
			return
		}
	}
}

func (s *IPCServer) admitIPCMessage(msg IPCMessage, frameBytes int) error {
	switch msg.Type {
	case MessageQuery, MessageQueryBatched, MessageQueryBlocked, MessageReserveQuery, MessageResolveQuery:
	default:
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.execCtx == nil {
		if s.execID != 0 || msg.ExecutionID != 0 {
			return errors.New("execution expired")
		}
	} else if msg.ExecutionID == 0 || msg.ExecutionID != s.execID {
		return errors.New("execution expired")
	}
	if s.execMessages >= maxIPCExecutionMessages || s.execBytes+int64(frameBytes) > maxIPCExecutionBytes {
		return errors.New("execution IPC quota exceeded")
	}
	s.execMessages++
	s.execBytes += int64(frameBytes)
	return nil
}

// handleMessage processes a single IPC message and returns the response.
func (s *IPCServer) handleMessage(msg IPCMessage) IPCMessage {
	switch msg.Type {
	case MessageQuery:
		return s.handleQuery(msg)
	case MessageQueryBatched:
		return s.handleQueryBatched(msg)
	case MessageQueryBlocked:
		return s.handleQueryBlocked(msg)
	case MessageReserveQuery:
		return s.handleReserveQuery(msg)
	case MessageResolveQuery:
		return s.handleResolveQuery(msg)
	case MessageReady:
		return IPCMessage{Type: MessageResponse, ID: msg.ID}
	case MessageExit:
		return IPCMessage{Type: MessageResponse, ID: msg.ID}
	default:
		return IPCMessage{
			Type:  MessageError,
			ID:    msg.ID,
			Error: fmt.Sprintf("unknown message type: %s", msg.Type),
		}
	}
}

type directIPCReservation struct {
	client LLMClient
	prompt string
}

func (r directIPCReservation) Resolve(ctx context.Context) (core.QueryResponse, error) {
	if r.client == nil {
		return core.QueryResponse{}, errors.New("IPC server has no LLM client")
	}
	return r.client.Query(ctx, r.prompt)
}

func (s *IPCServer) handleReserveQuery(msg IPCMessage) IPCMessage {
	queryCtx, cleanup, execID, _, ok := s.queryContext(msg.ExecutionID)
	defer cleanup()
	if !ok {
		return IPCMessage{Type: MessageError, ID: msg.ID, Error: "execution expired"}
	}
	s.mu.Lock()
	if s.execCtx == nil || s.execID != execID || len(s.reserved) >= maxIPCReservations {
		s.mu.Unlock()
		return IPCMessage{Type: MessageError, ID: msg.ID, Error: "async query reservation unavailable"}
	}
	s.reservation++
	reservationID := fmt.Sprintf("reservation-%d-%d", execID, s.reservation)
	s.reserved[reservationID] = reservedIPCQuery{executionID: execID, prompt: msg.Prompt}
	s.mu.Unlock()

	var reservation core.QueryReservation
	if client, supports := s.client.(core.ReservingLLMClient); supports {
		reservation = client.ReserveQuery(queryCtx, msg.Prompt)
	} else {
		reservation = directIPCReservation{client: s.client, prompt: msg.Prompt}
	}
	s.mu.Lock()
	reserved, exists := s.reserved[reservationID]
	if exists && reserved.executionID == execID && s.execCtx != nil && s.execID == execID {
		reserved.query = reservation
		s.reserved[reservationID] = reserved
	} else {
		delete(s.reserved, reservationID)
		exists = false
	}
	s.mu.Unlock()
	if !exists {
		return IPCMessage{Type: MessageError, ID: msg.ID, Error: "execution expired"}
	}
	return IPCMessage{Type: MessageResponse, ID: msg.ID, ReservationID: reservationID}
}

func (s *IPCServer) handleResolveQuery(msg IPCMessage) IPCMessage {
	queryCtx, cleanup, execID, activeExecution, ok := s.queryContext(msg.ExecutionID)
	defer cleanup()
	if !ok {
		return IPCMessage{Type: MessageError, ID: msg.ID, Error: "execution expired"}
	}
	s.mu.Lock()
	reserved, exists := s.reserved[msg.ReservationID]
	if exists && reserved.executionID == execID {
		delete(s.reserved, msg.ReservationID)
	} else {
		exists = false
	}
	s.mu.Unlock()
	if !exists {
		return IPCMessage{Type: MessageError, ID: msg.ID, Error: "unknown async query reservation"}
	}
	start := time.Now()
	result, err := reserved.query.Resolve(queryCtx)
	duration := time.Since(start).Seconds()
	if err != nil {
		s.recordCallForExecution(execID, activeExecution, reserved.prompt, fmt.Sprintf("Error: %v", err), duration, 0, 0)
		return IPCMessage{Type: MessageError, ID: msg.ID, Error: err.Error()}
	}
	s.recordCallForExecution(execID, activeExecution, reserved.prompt, result.Response, duration, result.PromptTokens, result.CompletionTokens)
	return IPCMessage{
		Type: MessageResponse, ID: msg.ID, Response: result.Response,
		TokenUsage: &TokenUsage{PromptTokens: result.PromptTokens, CompletionTokens: result.CompletionTokens},
	}
}

func (s *IPCServer) handleQueryBlocked(msg IPCMessage) IPCMessage {
	_, cleanup, execID, activeExecution, ok := s.queryContext(msg.ExecutionID)
	defer cleanup()
	if !ok {
		return IPCMessage{
			Type:  MessageError,
			ID:    msg.ID,
			Error: "execution expired",
		}
	}

	if len(msg.Prompts) > 0 {
		for i, prompt := range msg.Prompts {
			response := msg.Response
			if i < len(msg.Responses) {
				response = msg.Responses[i]
			}
			s.recordCallForExecution(execID, activeExecution, prompt, response, 0, 0, 0)
		}
		return IPCMessage{
			Type:      MessageResponse,
			ID:        msg.ID,
			Responses: msg.Responses,
		}
	}

	s.recordCallForExecution(execID, activeExecution, msg.Prompt, msg.Response, 0, 0, 0)
	return IPCMessage{
		Type:     MessageResponse,
		ID:       msg.ID,
		Response: msg.Response,
	}
}

// handleQuery processes a single Query() request.
func (s *IPCServer) handleQuery(msg IPCMessage) IPCMessage {
	start := time.Now()
	queryCtx, cleanup, execID, activeExecution, ok := s.queryContext(msg.ExecutionID)
	defer cleanup()
	if !ok {
		return IPCMessage{
			Type:  MessageError,
			ID:    msg.ID,
			Error: "execution expired",
		}
	}

	resp, err := s.client.Query(queryCtx, msg.Prompt)
	duration := time.Since(start).Seconds()

	if err != nil {
		s.recordCallForExecution(execID, activeExecution, msg.Prompt, fmt.Sprintf("Error: %v", err), duration, 0, 0)
		return IPCMessage{
			Type:  MessageError,
			ID:    msg.ID,
			Error: err.Error(),
		}
	}

	s.recordCallForExecution(execID, activeExecution, msg.Prompt, resp.Response, duration, resp.PromptTokens, resp.CompletionTokens)

	return IPCMessage{
		Type:     MessageResponse,
		ID:       msg.ID,
		Response: resp.Response,
		TokenUsage: &TokenUsage{
			PromptTokens:     resp.PromptTokens,
			CompletionTokens: resp.CompletionTokens,
		},
		Duration: duration,
	}
}

// handleQueryBatched processes a batched Query() request.
func (s *IPCServer) handleQueryBatched(msg IPCMessage) IPCMessage {
	start := time.Now()
	queryCtx, cleanup, execID, activeExecution, ok := s.queryContext(msg.ExecutionID)
	defer cleanup()
	if !ok {
		return IPCMessage{
			Type:  MessageError,
			ID:    msg.ID,
			Error: "execution expired",
		}
	}

	results, err := s.client.QueryBatched(queryCtx, msg.Prompts)
	duration := time.Since(start).Seconds()

	if err != nil {
		// Record each as failed
		for _, prompt := range msg.Prompts {
			s.recordCallForExecution(execID, activeExecution, prompt, fmt.Sprintf("Error: %v", err), durationPerPrompt(duration, len(msg.Prompts)), 0, 0)
		}
		return IPCMessage{
			Type:  MessageError,
			ID:    msg.ID,
			Error: err.Error(),
		}
	}

	responses := make([]string, len(results))
	usages := make([]TokenUsage, len(results))
	for i, r := range results {
		responses[i] = r.Response
		usages[i] = TokenUsage{
			PromptTokens:     r.PromptTokens,
			CompletionTokens: r.CompletionTokens,
		}
		s.recordCallForExecution(execID, activeExecution, msg.Prompts[i], r.Response, durationPerPrompt(duration, len(results)), r.PromptTokens, r.CompletionTokens)
	}

	return IPCMessage{
		Type:        MessageResponse,
		ID:          msg.ID,
		Responses:   responses,
		TokenUsages: usages,
		Duration:    duration,
	}
}

func (s *IPCServer) beginExecution(ctx context.Context) uint64 {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.execID++
	if s.execID == 0 {
		s.execID++
	}
	s.execCtx = ctx
	s.execMessages = 0
	s.execBytes = 0
	clear(s.reserved)
	return s.execID
}

func (s *IPCServer) endExecution(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if id != 0 && id == s.execID {
		s.execCtx = nil
		clear(s.reserved)
	}
}

func (s *IPCServer) queryContext(messageExecID uint64) (context.Context, context.CancelFunc, uint64, bool, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.execCtx != nil {
		if s.execCtx.Err() != nil {
			return context.Background(), func() {}, s.execID, true, false
		}
		if messageExecID != s.execID {
			return context.Background(), func() {}, s.execID, true, false
		}
		ctx, cancel := linkContexts(s.execCtx, s.ctx)
		return ctx, cancel, s.execID, true, true
	}
	if s.execID != 0 {
		return context.Background(), func() {}, s.execID, true, false
	}
	return s.ctx, func() {}, 0, false, true
}

func linkContexts(primary, secondary context.Context) (context.Context, context.CancelFunc) {
	if primary == nil {
		primary = context.Background()
	}
	if secondary == nil || primary == secondary {
		return primary, func() {}
	}
	ctx, cancel := context.WithCancel(primary)
	stop := context.AfterFunc(secondary, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

func (s *IPCServer) recordCallForExecution(execID uint64, activeExecution bool, prompt, response string, duration float64, promptTokens, completionTokens int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if activeExecution && (s.execCtx == nil || s.execID != execID) {
		return
	}
	if len(s.calls) >= maxIPCCallRecords {
		return
	}
	s.calls = append(s.calls, LLMCall{
		Prompt:           truncateRecordedText(prompt),
		Response:         truncateRecordedText(response),
		Duration:         duration,
		PromptTokens:     promptTokens,
		CompletionTokens: completionTokens,
	})
}

func truncateRecordedText(value string) string {
	if len(value) <= maxRecordedTextBytes {
		return value
	}
	return value[:maxRecordedTextBytes] + "[truncated]"
}

// GetCalls returns and clears the recorded LLM calls.
func (s *IPCServer) GetCalls() []LLMCall {
	s.mu.Lock()
	defer s.mu.Unlock()
	calls := s.calls
	s.calls = nil
	return calls
}

// ClearCalls clears the recorded LLM calls.
func (s *IPCServer) ClearCalls() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = nil
}

// Stop shuts down the server.
func (s *IPCServer) Stop() error {
	s.cancel()
	err := s.listener.Close()
	if s.socketDir != "" {
		if removeErr := os.RemoveAll(s.socketDir); err == nil {
			err = removeErr
		}
	}
	return err
}

// sendError sends an error response.
func (s *IPCServer) sendError(encoder *json.Encoder, id, errMsg string) {
	_ = encoder.Encode(IPCMessage{
		Type:  MessageError,
		ID:    id,
		Error: errMsg,
	})
}

// IPCClient is used by the sandbox to communicate with the host.
// This would be embedded in the container code.
type IPCClient struct {
	conn    net.Conn
	encoder *json.Encoder
	decoder *json.Decoder
	mu      sync.Mutex
	counter int
}

// NewIPCClient creates a new IPC client that connects to the host server.
func NewIPCClient(addr string) (*IPCClient, error) {
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to IPC server: %w", err)
	}

	return &IPCClient{
		conn:    conn,
		encoder: json.NewEncoder(conn),
		decoder: json.NewDecoder(conn),
	}, nil
}

// nextID generates a unique message ID.
func (c *IPCClient) nextID() string {
	c.counter++
	return fmt.Sprintf("msg-%d", c.counter)
}

// Query sends a Query request and waits for the response.
func (c *IPCClient) Query(prompt string) (string, *TokenUsage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	id := c.nextID()
	msg := IPCMessage{
		Type:   MessageQuery,
		ID:     id,
		Prompt: prompt,
	}

	if err := c.encoder.Encode(msg); err != nil {
		return "", nil, fmt.Errorf("failed to send query: %w", err)
	}

	var resp IPCMessage
	if err := c.decoder.Decode(&resp); err != nil {
		return "", nil, fmt.Errorf("failed to receive response: %w", err)
	}

	if resp.Type == MessageError {
		return "", nil, fmt.Errorf("query error: %s", resp.Error)
	}

	return resp.Response, resp.TokenUsage, nil
}

// QueryBatched sends a batched Query request.
func (c *IPCClient) QueryBatched(prompts []string) ([]string, []TokenUsage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	id := c.nextID()
	msg := IPCMessage{
		Type:    MessageQueryBatched,
		ID:      id,
		Prompts: prompts,
	}

	if err := c.encoder.Encode(msg); err != nil {
		return nil, nil, fmt.Errorf("failed to send batched query: %w", err)
	}

	var resp IPCMessage
	if err := c.decoder.Decode(&resp); err != nil {
		return nil, nil, fmt.Errorf("failed to receive response: %w", err)
	}

	if resp.Type == MessageError {
		return nil, nil, fmt.Errorf("query error: %s", resp.Error)
	}

	return resp.Responses, resp.TokenUsages, nil
}

// Close closes the connection.
func (c *IPCClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Send exit message
	_ = c.encoder.Encode(IPCMessage{Type: MessageExit})

	return c.conn.Close()
}

// GenerateContainerRLMCode generates Go code for the container that provides
// Query() and QueryBatched() functions that communicate via IPC.
func GenerateContainerRLMCode(ipcAddr string, finalTokens ...string) string {
	// Using backtick for struct tags
	bt := "`"
	finalToken := "test"
	if len(finalTokens) > 0 && finalTokens[0] != "" {
		finalToken = finalTokens[0]
	}
	var executionID uint64
	if len(finalTokens) > 1 && finalTokens[1] != "" {
		_, _ = fmt.Sscanf(finalTokens[1], "%d", &executionID)
	}
	maxFullContextQueryChars := 0
	if len(finalTokens) > 2 && finalTokens[2] != "" {
		_, _ = fmt.Sscanf(finalTokens[2], "%d", &maxFullContextQueryChars)
	}
	ipcNetwork := "tcp"
	if strings.HasPrefix(ipcAddr, "unix://") {
		ipcNetwork = "unix"
		ipcAddr = strings.TrimPrefix(ipcAddr, "unix://")
	}
	return fmt.Sprintf(`package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
	"unicode"
)

type messageType string

const (
	messageQuery        messageType = "query"
	messageQueryBatched messageType = "query_batched"
	messageQueryBlocked messageType = "query_blocked"
	messageReserveQuery messageType = "reserve_query"
	messageResolveQuery messageType = "resolve_query"
	messageResponse     messageType = "response"
	messageError        messageType = "error"
)

type tokenUsage struct {
	PromptTokens     int %sjson:"prompt_tokens"%s
	CompletionTokens int %sjson:"completion_tokens"%s
}

type ipcMessage struct {
	Type        messageType   %sjson:"type"%s
	ID          string        %sjson:"id,omitempty"%s
	ExecutionID uint64        %sjson:"execution_id,omitempty"%s
	Prompt      string        %sjson:"prompt,omitempty"%s
	Prompts     []string      %sjson:"prompts,omitempty"%s
	Response    string        %sjson:"response,omitempty"%s
	Responses   []string      %sjson:"responses,omitempty"%s
	Error       string        %sjson:"error,omitempty"%s
	TokenUsage  *tokenUsage   %sjson:"token_usage,omitempty"%s
	TokenUsages []tokenUsage  %sjson:"token_usages,omitempty"%s
	ReservationID string      %sjson:"reservation_id,omitempty"%s
}

var (
	ipcConn    net.Conn
	ipcEncoder *json.Encoder
	ipcReader  *bufio.Reader
	ipcMu      sync.Mutex
	ipcCounter int
	ipcNetwork = %q
	ipcAddr    = %q
	ipcExecutionID uint64 = %d
)

const maxFullContextQueryChars = %d
const maxContainerPromptBytes = 1 << 20
const maxContainerBatch = 64
const maxAsyncHandles = 64
const maxIPCFrameBytes = 2 << 20

func readIPCLine(reader *bufio.Reader) ([]byte, error) {
	frame := make([]byte, 0, 4096)
	for {
		fragment, err := reader.ReadSlice('\n')
		if len(frame)+len(fragment) > maxIPCFrameBytes {
			return nil, fmt.Errorf("IPC frame exceeds %%d bytes", maxIPCFrameBytes)
		}
		frame = append(frame, fragment...)
		if err == nil {
			return frame, nil
		}
		if err != bufio.ErrBufferFull {
			return nil, err
		}
	}
}

func init() {
	var err error
	ipcConn, err = net.DialTimeout(ipcNetwork, ipcAddr, 10*time.Second)
	if err != nil {
		panic(fmt.Sprintf("failed to connect to IPC server: %%v", err))
	}
	ipcEncoder = json.NewEncoder(ipcConn)
	ipcReader = bufio.NewReader(ipcConn)
}

func nextID() string {
	ipcCounter++
	return fmt.Sprintf("msg-%%d", ipcCounter)
}

func buildPromptWithContext(prompt string) string {
	return buildPromptWithProvidedContext(context, prompt)
}

func buildPromptWithProvidedContext(contextStr, prompt string) string {
	if contextStr == "" {
		return prompt
	}
	return fmt.Sprintf("Context data:\n%%s\n\nTask: %%s\n\nIMPORTANT: Provide a direct, concise answer. Do not explain your reasoning unless specifically asked.", contextStr, prompt)
}

func fullContextQueryBlocked(name string) string {
	if maxFullContextQueryChars <= 0 || len(context) <= maxFullContextQueryChars {
		return ""
	}
	return fmt.Sprintf("%%s would prepend the full context (%%d chars), exceeding the limit of %%d chars; use QueryWith(contextSlice, prompt) or QueryRaw(prompt)", name, len(context), maxFullContextQueryChars)
}

func contextChunks() []string {
	if context == "" {
		return nil
	}
	const chunkSize = 4000
	const overlap = 200
	byteOffsets := runeByteOffsets(context)
	runeCount := len(byteOffsets) - 1
	var chunks []string
	for start := 0; start < runeCount; {
		end := start + chunkSize
		if end > runeCount {
			end = runeCount
		}
		chunks = append(chunks, context[byteOffsets[start]:byteOffsets[end]])
		if end == runeCount {
			break
		}
		start = end - overlap
		if start < 0 {
			start = 0
		}
	}
	return chunks
}

func runeByteOffsets(raw string) []int {
	offsets := make([]int, 0, len(raw)+1)
	for offset := range raw {
		offsets = append(offsets, offset)
	}
	offsets = append(offsets, len(raw))
	return offsets
}

func FindRelevant(query string, topK int) []string {
	chunks := contextChunks()
	if len(chunks) == 0 {
		return []string{}
	}
	if topK <= 0 {
		topK = 3
	}
	if topK > len(chunks) {
		topK = len(chunks)
	}
	terms := contextSearchTerms(query)
	if len(terms) == 0 {
		return chunks[:topK]
	}
	results := make([]string, 0, topK)
	used := make([]bool, len(chunks))
	for len(results) < topK {
		bestIdx, bestScore := -1, 0
		for i, chunk := range chunks {
			if used[i] {
				continue
			}
			score := 0
			lower := strings.ToLower(chunk)
			for _, term := range terms {
				score += strings.Count(lower, term)
			}
			if bestIdx == -1 || score > bestScore {
				bestIdx, bestScore = i, score
			}
		}
		if bestIdx == -1 || bestScore == 0 {
			break
		}
		used[bestIdx] = true
		results = append(results, chunks[bestIdx])
	}
	if len(results) == 0 {
		return chunks[:topK]
	}
	return results
}

func contextSearchTerms(query string) []string {
	fields := strings.FieldsFunc(strings.ToLower(query), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
	out := make([]string, 0, len(fields))
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if len(field) < 2 {
			continue
		}
		if _, ok := seen[field]; ok {
			continue
		}
		seen[field] = struct{}{}
		out = append(out, field)
	}
	return out
}

func GetChunk(id int) string {
	chunks := contextChunks()
	if id < 0 || id >= len(chunks) {
		return ""
	}
	return chunks[id]
}

func GetContext(startLine, endLine int) string {
	lines := strings.Split(context, "\n")
	if len(lines) == 0 || context == "" {
		return ""
	}
	if startLine < 1 {
		startLine = 1
	}
	if startLine > len(lines) {
		return ""
	}
	if endLine < startLine {
		endLine = startLine
	}
	if endLine > len(lines) {
		endLine = len(lines)
	}
	return strings.Join(lines[startLine-1:endLine], "\n")
}

func ChunkCount() int {
	return len(contextChunks())
}

func LineCount() int {
	if context == "" {
		return 0
	}
	return len(strings.Split(context, "\n"))
}

func queryRaw(prompt string) string {
	if len(prompt) > maxContainerPromptBytes {
		return fmt.Sprintf("Error: query prompt exceeds %%d bytes", maxContainerPromptBytes)
	}
	ipcMu.Lock()
	defer ipcMu.Unlock()

	msg := ipcMessage{
		Type:        messageQuery,
		ID:          nextID(),
		ExecutionID: ipcExecutionID,
		Prompt:      prompt,
	}

	if err := ipcEncoder.Encode(msg); err != nil {
		return fmt.Sprintf("Error: failed to send query: %%v", err)
	}

	line, err := readIPCLine(ipcReader)
	if err != nil {
		return fmt.Sprintf("Error: failed to receive response: %%v", err)
	}

	var resp ipcMessage
	if err := json.Unmarshal(line, &resp); err != nil {
		return fmt.Sprintf("Error: failed to parse response: %%v", err)
	}

	if resp.Type == messageError {
		return fmt.Sprintf("Error: %%s", resp.Error)
	}

	return resp.Response
}

func recordBlockedQuery(prompt, response string) {
	recordBlockedQueries([]string{prompt}, []string{response})
}

func recordBlockedQueries(prompts, responses []string) {
	ipcMu.Lock()
	defer ipcMu.Unlock()

	msg := ipcMessage{
		Type:        messageQueryBlocked,
		ID:          nextID(),
		ExecutionID: ipcExecutionID,
		Prompts:     prompts,
		Responses:   responses,
	}

	if len(prompts) == 1 && len(responses) == 1 {
		msg.Prompt = prompts[0]
		msg.Response = responses[0]
	}

	if err := ipcEncoder.Encode(msg); err != nil {
		return
	}
	_, _ = readIPCLine(ipcReader)
}

// Query sends a query with the full context prepended to the host LLM.
func Query(prompt string) string {
	if err := fullContextQueryBlocked("Query"); err != "" {
		response := "Error: " + err
		recordBlockedQuery(prompt, response)
		return response
	}
	return queryRaw(buildPromptWithContext(prompt))
}

func QueryRaw(prompt string) string {
	return queryRaw(prompt)
}

func QueryWith(contextSlice, prompt string) string {
	return queryRaw(buildPromptWithProvidedContext(contextSlice, prompt))
}

func queryBatchedRaw(prompts []string) []string {
	if len(prompts) > maxContainerBatch {
		return []string{fmt.Sprintf("Error: query batch exceeds %%d prompts", maxContainerBatch)}
	}
	totalBytes := 0
	for _, prompt := range prompts {
		totalBytes += len(prompt)
		if len(prompt) > maxContainerPromptBytes || totalBytes > maxContainerPromptBytes {
			return []string{fmt.Sprintf("Error: query batch exceeds %%d prompt bytes", maxContainerPromptBytes)}
		}
	}
	ipcMu.Lock()
	defer ipcMu.Unlock()

	msg := ipcMessage{
		Type:        messageQueryBatched,
		ID:          nextID(),
		ExecutionID: ipcExecutionID,
		Prompts:     prompts,
	}

	if err := ipcEncoder.Encode(msg); err != nil {
		results := make([]string, len(prompts))
		for i := range results {
			results[i] = fmt.Sprintf("Error: failed to send query: %%v", err)
		}
		return results
	}

	line, err := readIPCLine(ipcReader)
	if err != nil {
		results := make([]string, len(prompts))
		for i := range results {
			results[i] = fmt.Sprintf("Error: failed to receive response: %%v", err)
		}
		return results
	}

	var resp ipcMessage
	if err := json.Unmarshal(line, &resp); err != nil {
		results := make([]string, len(prompts))
		for i := range results {
			results[i] = fmt.Sprintf("Error: failed to parse response: %%v", err)
		}
		return results
	}

	if resp.Type == messageError {
		results := make([]string, len(prompts))
		for i := range results {
			results[i] = fmt.Sprintf("Error: %%s", resp.Error)
		}
		return results
	}

	return resp.Responses
}

// QueryBatched sends multiple full-context queries and returns the responses.
func QueryBatched(prompts []string) []string {
	if len(prompts) > maxContainerBatch {
		return []string{fmt.Sprintf("Error: query batch exceeds %%d prompts", maxContainerBatch)}
	}
	if err := fullContextQueryBlocked("QueryBatched"); err != "" {
		results := make([]string, len(prompts))
		for i := range results {
			results[i] = "Error: " + err
		}
		recordBlockedQueries(prompts, results)
		return results
	}
	fullPrompts := make([]string, len(prompts))
	for i, prompt := range prompts {
		fullPrompts[i] = buildPromptWithContext(prompt)
	}
	return queryBatchedRaw(fullPrompts)
}

func QueryBatchedRaw(prompts []string) []string {
	return queryBatchedRaw(prompts)
}

func reserveAsyncQuery(prompt string) string {
	if len(prompt) > maxContainerPromptBytes {
		return fmt.Sprintf("Error: query prompt exceeds %%d bytes", maxContainerPromptBytes)
	}
	ipcMu.Lock()
	defer ipcMu.Unlock()
	msg := ipcMessage{Type: messageReserveQuery, ID: nextID(), ExecutionID: ipcExecutionID, Prompt: prompt}
	if err := ipcEncoder.Encode(msg); err != nil {
		return fmt.Sprintf("Error: failed to reserve query: %%v", err)
	}
	line, err := readIPCLine(ipcReader)
	if err != nil {
		return fmt.Sprintf("Error: failed to receive reservation: %%v", err)
	}
	var resp ipcMessage
	if err := json.Unmarshal(line, &resp); err != nil {
		return fmt.Sprintf("Error: failed to parse reservation: %%v", err)
	}
	if resp.Type == messageError {
		return fmt.Sprintf("Error: %%s", resp.Error)
	}
	return resp.ReservationID
}

func resolveAsyncQuery(reservationID string) string {
	conn, err := net.DialTimeout(ipcNetwork, ipcAddr, 10*time.Second)
	if err != nil {
		return fmt.Sprintf("Error: failed to connect to IPC server: %%v", err)
	}
	defer conn.Close()
	encoder := json.NewEncoder(conn)
	reader := bufio.NewReader(conn)
	ipcMu.Lock()
	messageID := nextID()
	ipcMu.Unlock()
	msg := ipcMessage{
		Type: messageResolveQuery, ID: messageID, ExecutionID: ipcExecutionID,
		ReservationID: reservationID,
	}
	if err := encoder.Encode(msg); err != nil {
		return fmt.Sprintf("Error: failed to resolve query: %%v", err)
	}
	line, err := readIPCLine(reader)
	if err != nil {
		return fmt.Sprintf("Error: failed to receive response: %%v", err)
	}
	var resp ipcMessage
	if err := json.Unmarshal(line, &resp); err != nil {
		return fmt.Sprintf("Error: failed to parse response: %%v", err)
	}
	if resp.Type == messageError {
		return fmt.Sprintf("Error: %%s", resp.Error)
	}
	return resp.Response
}

func reserveAsync(prompt string) string {
	if err := fullContextQueryBlocked("QueryAsync"); err != "" {
		response := "Error: " + err
		recordBlockedQuery(prompt, response)
		return response
	}
	return reserveAsyncQuery(buildPromptWithContext(prompt))
}

type asyncQueryResult struct {
	done     chan struct{}
	response string
}

var (
	asyncMu      sync.Mutex
	asyncCounter int
	asyncQueries = make(map[string]*asyncQueryResult)
)

func QueryAsync(prompt string) string {
	asyncMu.Lock()
	if len(asyncQueries) >= maxAsyncHandles {
		asyncMu.Unlock()
		return "Error: too many outstanding async queries"
	}
	asyncCounter++
	handle := fmt.Sprintf("async-%%d", asyncCounter)
	result := &asyncQueryResult{done: make(chan struct{})}
	asyncQueries[handle] = result
	asyncMu.Unlock()
	reservationID := reserveAsync(prompt)
	if strings.HasPrefix(reservationID, "Error:") {
		asyncMu.Lock()
		delete(asyncQueries, handle)
		asyncMu.Unlock()
		return reservationID
	}
	go func() {
		result.response = resolveAsyncQuery(reservationID)
		close(result.done)
	}()
	return handle
}

func QueryBatchedAsync(prompts []string) []string {
	if len(prompts) > maxContainerBatch {
		return []string{fmt.Sprintf("Error: query batch exceeds %%d prompts", maxContainerBatch)}
	}
	handles := make([]string, len(prompts))
	for i, prompt := range prompts {
		handles[i] = QueryAsync(prompt)
	}
	return handles
}

func WaitAsync(handle string) string {
	if strings.HasPrefix(handle, "Error:") {
		return handle
	}
	asyncMu.Lock()
	result := asyncQueries[handle]
	asyncMu.Unlock()
	if result == nil {
		return "Error: unknown async query handle"
	}
	<-result.done
	asyncMu.Lock()
	delete(asyncQueries, handle)
	asyncMu.Unlock()
	return result.response
}

var FINAL = func() func(any) string {
	finalPrefix := %q
	finalToken := %q
	return func(value any) string {
		finalValue := fmt.Sprint(value)
		encoded := base64.StdEncoding.EncodeToString([]byte(finalValue))
		fmt.Printf("\n%%s%%s__%%s\n", finalPrefix, finalToken, encoded)
		return finalValue
	}
}()

var FINAL_VAR = FINAL
`,
		bt, bt, bt, bt, // tokenUsage (2 fields x 2 backticks)
		bt, bt, bt, bt, bt, bt, bt, bt, bt, bt, bt, bt, bt, bt, bt, bt, bt, bt, bt, bt, bt, bt, // ipcMessage (11 fields x 2 backticks)
		ipcNetwork, ipcAddr, executionID, maxFullContextQueryChars,
		finalMarkerPrefix, finalToken,
	)
}
