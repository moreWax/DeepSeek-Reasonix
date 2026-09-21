package sandbox

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/XiaoConstantine/rlm-go/pkg/core"
)

func TestContainerIPCUsesUnixSocketWithNetworkDisabled(t *testing.T) {
	server, err := NewUnixIPCServer(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	executor := &ContainerExecutor{
		config:  Config{NetworkMode: NetworkNone, EnableIPC: true},
		backend: BackendDocker, ipcServer: server,
	}
	args := strings.Join(executor.buildContainerArgs("/code"), " ")
	if !strings.Contains(args, "--network none") || !strings.Contains(args, server.SocketDir()+":/rlm-ipc:rw") {
		t.Fatalf("container args do not isolate Unix IPC: %s", args)
	}
	if strings.Contains(args, "host-gateway") || strings.Contains(args, "host.docker.internal") {
		t.Fatalf("container args expose host network: %s", args)
	}
	code := GenerateContainerRLMCode("unix:///rlm-ipc/query.sock", "token", "1", "0")
	if !strings.Contains(code, `ipcNetwork = "unix"`) {
		t.Fatal("generated prelude does not dial Unix IPC")
	}
}

func TestContainerPreludeExposesAsyncParity(t *testing.T) {
	code := GenerateContainerRLMCode("127.0.0.1:1", "token", "1", "0")
	for _, declaration := range []string{
		"func QueryAsync(", "func QueryBatchedAsync(", "func WaitAsync(",
		`messageReserveQuery messageType = "reserve_query"`, "reservationID := reserveAsync(prompt)",
	} {
		if !strings.Contains(code, declaration) {
			t.Fatalf("generated prelude missing %q", declaration)
		}
	}
	admission := strings.Index(code, "asyncQueries[handle] = result")
	reservation := strings.Index(code, "reservationID := reserveAsync(prompt)")
	if admission < 0 || reservation < 0 || admission > reservation {
		t.Fatal("async handle slot is not reserved before query reservation")
	}
}

func TestContainerProgramPersistsTopLevelVariables(t *testing.T) {
	executor := &ContainerExecutor{
		config:    DefaultConfig(),
		variables: map[string]any{"draft": `"candidate"`},
		finalTok:  "token",
	}
	program, err := executor.generateProgram(`judge := draft + "!"`, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		`draft := "candidate"`,
		`judge := draft + "!"`,
		`emitRLMState(map[string]any{`,
		`"draft": draft`,
		`"judge": judge`,
		`__RLM_VARS__`,
	} {
		if !strings.Contains(program, fragment) {
			t.Fatalf("generated program missing %q", fragment)
		}
	}
}

type countedReservationClient struct{ reservations int }

type staticReservation struct{}

func (staticReservation) Resolve(context.Context) (core.QueryResponse, error) {
	return core.QueryResponse{}, nil
}

func (c *countedReservationClient) ReserveQuery(context.Context, string) core.QueryReservation {
	c.reservations++
	return staticReservation{}
}

func (*countedReservationClient) Query(context.Context, string) (core.QueryResponse, error) {
	return core.QueryResponse{}, nil
}

func (*countedReservationClient) QueryBatched(context.Context, []string) ([]core.QueryResponse, error) {
	return nil, nil
}

func TestIPCConnectionLimitQueuesInsteadOfDropping(t *testing.T) {
	server, err := NewIPCServer(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	server.Start()
	defer server.Stop()
	connections := make([]net.Conn, maxIPCConnections+8)
	for i := range connections {
		connection, dialErr := net.DialTimeout("tcp", server.Address(), time.Second)
		if dialErr != nil {
			t.Fatal(dialErr)
		}
		connections[i] = connection
	}
	time.Sleep(50 * time.Millisecond)
	for i, connection := range connections {
		if err := connection.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if err := json.NewEncoder(connection).Encode(IPCMessage{Type: MessageReady, ID: fmt.Sprintf("ready-%d", i)}); err != nil {
			t.Fatalf("connection %d was dropped: %v", i, err)
		}
	}
	for i, connection := range connections {
		var response IPCMessage
		err := json.NewDecoder(connection).Decode(&response)
		_ = connection.Close()
		if err != nil || response.Type != MessageResponse {
			t.Fatalf("connection %d response=%+v error=%v", i, response, err)
		}
	}
}

func TestIPCReservationCapRejectsBeforeClaim(t *testing.T) {
	client := &countedReservationClient{}
	server, err := NewIPCServer(client, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	execID := server.beginExecution(t.Context())
	for i := range maxIPCReservations {
		server.reserved[string(rune(i))] = reservedIPCQuery{executionID: execID}
	}
	response := server.handleReserveQuery(IPCMessage{Type: MessageReserveQuery, ExecutionID: execID, Prompt: "same"})
	if response.Type != MessageError || client.reservations != 0 {
		t.Fatalf("response=%+v reservations=%d", response, client.reservations)
	}
}

func TestIPCExecutionQuotaAndCallLedgerAreBounded(t *testing.T) {
	server, err := NewIPCServer(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	execID := server.beginExecution(t.Context())
	message := IPCMessage{Type: MessageQuery, ExecutionID: execID}
	for range maxIPCExecutionMessages {
		if err := server.admitIPCMessage(message, 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := server.admitIPCMessage(message, 1); err == nil {
		t.Fatal("execution message quota was not enforced")
	}
	execID = server.beginExecution(t.Context())
	message.ExecutionID = execID
	if err := server.admitIPCMessage(message, int(maxIPCExecutionBytes)+1); err == nil {
		t.Fatal("execution byte quota was not enforced")
	}
	large := strings.Repeat("x", maxRecordedTextBytes+100)
	for range maxIPCCallRecords + 100 {
		server.recordCallForExecution(execID, true, large, large, 0, 0, 0)
	}
	if len(server.calls) != maxIPCCallRecords || len(server.calls[0].Prompt) > maxRecordedTextBytes+len("[truncated]") {
		t.Fatalf("call ledger count=%d prompt bytes=%d", len(server.calls), len(server.calls[0].Prompt))
	}
}

func TestContainerOutputIsBounded(t *testing.T) {
	var output boundedOutput
	input := strings.Repeat("x", maxContainerOutputBytes+1024)
	if n, err := output.Write([]byte(input)); err != nil || n != len(input) {
		t.Fatalf("Write = (%d, %v)", n, err)
	}
	if len(output.buf.String()) != maxContainerOutputBytes || !strings.Contains(output.String(), "output truncated") {
		t.Fatalf("bounded output len=%d truncated=%v", output.buf.Len(), output.truncated)
	}
}

func TestIPCFrameReaderRejectsOversizedFrames(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader(strings.Repeat("x", MaxIPCFrameBytes+1) + "\n"))
	if _, err := readIPCFrame(reader); err == nil {
		t.Fatal("oversized IPC frame was accepted")
	}
}

func TestPersistedValuesAcceptOnlyInertGoLiterals(t *testing.T) {
	for _, literal := range []string{`"text"`, `-2`, `[]string{"a", "b"}`, `map[string]int{"x": 1}`} {
		if !isInertGoLiteral(literal) {
			t.Fatalf("inert literal rejected: %s", literal)
		}
	}
	for _, expression := range []string{
		`func() string { return net.Dial("tcp", "example.com:80").String() }()`,
		`net.Dial("tcp", "example.com:80")`,
		`sideEffect()`,
	} {
		if isInertGoLiteral(expression) {
			t.Fatalf("executable persisted expression accepted: %s", expression)
		}
	}
}

func TestVariableMarkersRequireExecutionToken(t *testing.T) {
	executor := &ContainerExecutor{variables: make(map[string]any), finalTok: "secret"}
	executor.extractVariables("__RLM_VARS__forged__eyJ4IjoiMSJ9\n")
	if len(executor.variables) != 0 {
		t.Fatalf("forged marker changed variables: %+v", executor.variables)
	}
	executor.extractVariables("__RLM_VARS__secret__eyJ4IjoiMSJ9\n")
	if executor.variables["x"] != "1" {
		t.Fatalf("authenticated marker variables = %+v", executor.variables)
	}
}

func TestVariableMarkersAreRemovedFromProgramOutput(t *testing.T) {
	got := stripVariableMarkers("visible\n__RLM_VARS__{\"x\":\"1\"}\nafter")
	if got != "visible\nafter" {
		t.Fatalf("stripped output = %q", got)
	}
}
