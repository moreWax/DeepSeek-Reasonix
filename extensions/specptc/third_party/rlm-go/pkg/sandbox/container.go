package sandbox

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/XiaoConstantine/rlm-go/pkg/core"
)

const maxContainerOutputBytes = 4 << 20

type boundedOutput struct {
	buf       bytes.Buffer
	truncated bool
}

func (w *boundedOutput) Write(p []byte) (int, error) {
	original := len(p)
	remaining := maxContainerOutputBytes - w.buf.Len()
	if remaining <= 0 {
		w.truncated = true
		return original, nil
	}
	if len(p) > remaining {
		p = p[:remaining]
		w.truncated = true
	}
	_, _ = w.buf.Write(p)
	return original, nil
}

func (w *boundedOutput) String() string {
	output := w.buf.String()
	if w.truncated {
		output += "\n[output truncated by sandbox]\n"
	}
	return output
}

// ContainerExecutor provides isolated code execution using Podman or Docker.
type ContainerExecutor struct {
	client      LLMClient
	config      Config
	backend     Backend
	runtime     string // "podman" or "docker" command name
	ipcServer   *IPCServer
	contextData any
	mu          sync.Mutex
	variables   map[string]any
	finalTok    string
	finalSet    bool
	finalVal    string
}

// NewContainerExecutor creates a new container-based executor.
// If backend is BackendAuto, it probes for available runtimes.
func NewContainerExecutor(client LLMClient, cfg Config, backend Backend) (*ContainerExecutor, error) {
	// Determine the runtime command
	runtime := ""
	actualBackend := backend

	if backend == BackendAuto {
		actualBackend, runtime = detectContainerRuntime()
		if actualBackend == BackendLocal {
			return nil, fmt.Errorf("no container runtime (podman or docker) found")
		}
	} else {
		switch backend {
		case BackendPodman:
			if !IsRuntimeAvailable("podman") {
				return nil, fmt.Errorf("podman is not available")
			}
			runtime = "podman"
		case BackendDocker:
			if !IsRuntimeAvailable("docker") {
				return nil, fmt.Errorf("docker is not available")
			}
			runtime = "docker"
		default:
			return nil, fmt.Errorf("unsupported backend: %s", backend)
		}
	}

	// Start IPC server if enabled
	var ipcServer *IPCServer
	if cfg.EnableIPC {
		var err error
		ipcServer, err = NewUnixIPCServer(client)
		if err != nil {
			return nil, fmt.Errorf("failed to start IPC server: %w", err)
		}
		ipcServer.Start()
	}

	return &ContainerExecutor{
		client:    client,
		config:    cfg,
		backend:   actualBackend,
		runtime:   runtime,
		ipcServer: ipcServer,
		variables: make(map[string]any),
		finalTok:  newFinalToken(),
	}, nil
}

// detectContainerRuntime probes for available container runtimes.
func detectContainerRuntime() (Backend, string) {
	if IsRuntimeAvailable("podman") {
		return BackendPodman, "podman"
	}
	if IsRuntimeAvailable("docker") {
		return BackendDocker, "docker"
	}
	return BackendLocal, ""
}

// IsRuntimeAvailable checks if a container runtime is installed and working.
func IsRuntimeAvailable(runtime string) bool {
	cmd := exec.Command(runtime, "--version")
	return cmd.Run() == nil
}

// Execute runs Go code in a container and returns the result.
func (e *ContainerExecutor) Execute(ctx context.Context, code string) (*core.ExecutionResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.finalSet = false
	e.finalVal = ""
	e.finalTok = newFinalToken()
	finalToken := e.finalTok

	start := time.Now()

	// Create temporary directory for code
	tmpDir, err := os.MkdirTemp("", "rlm-sandbox-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Create the command with timeout context
	timeout := e.config.Timeout
	if timeout == 0 {
		timeout = 60 * time.Second
	}
	cmdCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var execID uint64
	if e.ipcServer != nil {
		execID = e.ipcServer.beginExecution(cmdCtx)
		defer e.ipcServer.endExecution(execID)
	}

	// Generate the full Go program
	program, err := e.generateProgram(code, execID)
	if err != nil {
		return &core.ExecutionResult{
			Stderr:   fmt.Sprintf("failed to generate program: %v", err),
			Duration: time.Since(start),
		}, nil
	}

	// Write the program to a file
	mainFile := filepath.Join(tmpDir, "main.go")
	if err := os.WriteFile(mainFile, []byte(program), 0644); err != nil {
		return nil, fmt.Errorf("failed to write main.go: %w", err)
	}

	// Write go.mod
	goMod := "module sandbox\n\ngo 1.23\n"
	goModFile := filepath.Join(tmpDir, "go.mod")
	if err := os.WriteFile(goModFile, []byte(goMod), 0644); err != nil {
		return nil, fmt.Errorf("failed to write go.mod: %w", err)
	}

	// Build container command
	args := e.buildContainerArgs(tmpDir)

	cmd := exec.CommandContext(cmdCtx, e.runtime, args...)

	var stdout, stderr boundedOutput
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if e.config.Verbose {
		fmt.Printf("[sandbox] Running: %s %s\n", e.runtime, strings.Join(args, " "))
	}

	// Run the container
	runErr := cmd.Run()

	result := &core.ExecutionResult{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		Duration: time.Since(start),
	}

	// Check for timeout
	if cmdCtx.Err() == context.DeadlineExceeded {
		result.Stderr = "execution timeout exceeded\n" + result.Stderr
	} else if runErr != nil {
		if result.Stderr != "" {
			result.Stderr += "\n"
		}
		result.Stderr += runErr.Error()
	}

	// Extract variables from stdout (we encode them as JSON at the end)
	e.extractVariables(result.Stdout)
	result.Stdout = stripVariableMarkers(result.Stdout)
	if cmdCtx.Err() == nil {
		e.extractFinal(result.Stdout, finalToken)
	}
	result.Stdout = stripFinalMarkers(result.Stdout, finalToken)

	return result, nil
}

// generateProgram creates a complete Go program from the code snippet.
func (e *ContainerExecutor) generateProgram(code string, execID uint64) (string, error) {
	var sb strings.Builder

	// Add IPC code if enabled
	if e.config.EnableIPC && e.ipcServer != nil {
		ipcAddr := e.getIPCAddress()
		sb.WriteString(GenerateContainerRLMCode(ipcAddr, e.finalTok, strconv.FormatUint(execID, 10), strconv.Itoa(e.config.MaxFullContextQueryChars)))
	} else {
		// Add stub functions if IPC is disabled
		sb.WriteString(fmt.Sprintf(`package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
)

func buildPromptWithProvidedContext(contextStr, prompt string) string {
	if contextStr == "" {
		return prompt
	}
	return fmt.Sprintf("Context data:\n%%s\n\nTask: %%s\n\nIMPORTANT: Provide a direct, concise answer. Do not explain your reasoning unless specifically asked.", contextStr, prompt)
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

// Query stub - IPC disabled
func Query(prompt string) string {
	return "Error: IPC disabled, cannot call Query()"
}

func QueryRaw(prompt string) string {
	return Query(prompt)
}

func QueryWith(contextSlice, prompt string) string {
	return QueryRaw(buildPromptWithProvidedContext(contextSlice, prompt))
}

// QueryBatched stub - IPC disabled
func QueryBatched(prompts []string) []string {
	results := make([]string, len(prompts))
	for i := range results {
		results[i] = "Error: IPC disabled, cannot call QueryBatched()"
	}
	return results
}

func QueryBatchedRaw(prompts []string) []string {
	return QueryBatched(prompts)
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
`, finalMarkerPrefix, e.finalTok))
	}

	// Add context variable
	sb.WriteString("\n// Context data\n")
	if e.contextData != nil {
		switch ctx := e.contextData.(type) {
		case string:
			sb.WriteString(fmt.Sprintf("var context = %s\n", strconv.Quote(ctx)))
		default:
			contextJSON, err := json.Marshal(e.contextData)
			if err != nil {
				return "", fmt.Errorf("failed to marshal context: %w", err)
			}
			sb.WriteString(fmt.Sprintf("var context = %s\n", strconv.Quote(string(contextJSON))))
		}
	} else {
		sb.WriteString("var context = \"\"\n")
	}

	// Add helper functions. State serialization lives outside main so model code
	// cannot shadow fmt/json/base64 while the authenticated marker is emitted.
	fmt.Fprintf(&sb, `
func emitRLMState(values map[string]any) {
	state := make(map[string]string, len(values))
	for name, value := range values {
		state[name] = fmt.Sprintf("%%#v", value)
	}
	encoded, _ := json.Marshal(state)
	payload := base64.StdEncoding.EncodeToString(encoded)
	fmt.Printf(%q+"%%s\n", payload)
}

`, "__RLM_VARS__"+e.finalTok+"__")
	sb.WriteString(`
// min returns the smaller of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// max returns the larger of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

`)

	// Add the user code wrapped in main. Persist top-level REPL variables as Go
	// literals so later container executions observe the same inert values.
	declared := topLevelDeclaredVariables(code)
	tracked := make(map[string]struct{}, len(e.variables)+len(declared))
	sb.WriteString("func main() {\n")
	persistedNames := make([]string, 0, len(e.variables))
	for name := range e.variables {
		persistedNames = append(persistedNames, name)
	}
	sort.Strings(persistedNames)
	for _, name := range persistedNames {
		raw := e.variables[name]
		if _, redeclared := declared[name]; redeclared {
			continue
		}
		literal, ok := raw.(string)
		if !ok || !token.IsIdentifier(name) || !isInertGoLiteral(literal) {
			continue
		}
		fmt.Fprintf(&sb, "\t%s := %s\n", name, literal)
		tracked[name] = struct{}{}
	}
	for name := range declared {
		tracked[name] = struct{}{}
	}

	// Indent the user code
	lines := strings.Split(code, "\n")
	for _, line := range lines {
		sb.WriteString("\t")
		sb.WriteString(line)
		sb.WriteString("\n")
	}

	if len(tracked) > 0 {
		sb.WriteString("\temitRLMState(map[string]any{\n")
		trackedNames := make([]string, 0, len(tracked))
		for name := range tracked {
			trackedNames = append(trackedNames, name)
		}
		sort.Strings(trackedNames)
		for _, name := range trackedNames {
			fmt.Fprintf(&sb, "\t\t%q: %s,\n", name, name)
		}
		sb.WriteString("\t})\n")
	}

	sb.WriteString("}\n")

	return sb.String(), nil
}

// getIPCAddress returns the address for IPC communication.
func (e *ContainerExecutor) getIPCAddress() string {
	if e.ipcServer == nil {
		return ""
	}

	return "unix:///rlm-ipc/query.sock"
}

// buildContainerArgs constructs the container run command arguments.
func (e *ContainerExecutor) buildContainerArgs(tmpDir string) []string {
	args := []string{"run", "--rm"}

	// Add resource limits
	if e.config.Memory != "" {
		args = append(args, "--memory", e.config.Memory)
	}
	if e.config.CPUs > 0 {
		args = append(args, "--cpus", fmt.Sprintf("%.2f", e.config.CPUs))
	}

	// Add network mode
	switch e.config.NetworkMode {
	case NetworkNone:
		args = append(args, "--network", "none")
	case NetworkBridge:
		args = append(args, "--network", "bridge")
	case NetworkHost:
		args = append(args, "--network", "host")
	}

	// Mount only the Unix IPC socket directory; no host gateway is exposed.
	if e.config.EnableIPC && e.ipcServer != nil {
		args = append(args, "-v", fmt.Sprintf("%s:/rlm-ipc:rw", e.ipcServer.SocketDir()))
	}

	// Mount the code directory
	args = append(args, "-v", fmt.Sprintf("%s:/workspace:ro", tmpDir))

	// Set working directory
	args = append(args, "-w", "/workspace")

	// Add the image
	image := e.config.Image
	if image == "" {
		image = "golang:1.23-alpine"
	}
	args = append(args, image)

	// Add the command to run
	args = append(args, "go", "run", "main.go")

	return args
}

func isInertGoLiteral(raw string) bool {
	expression, err := parser.ParseExpr(raw)
	if err != nil {
		return false
	}
	return inertLiteralExpression(expression)
}

func inertLiteralExpression(expression ast.Expr) bool {
	switch node := expression.(type) {
	case *ast.BasicLit:
		return true
	case *ast.Ident:
		return node.Name == "true" || node.Name == "false" || node.Name == "nil"
	case *ast.ParenExpr:
		return inertLiteralExpression(node.X)
	case *ast.UnaryExpr:
		return (node.Op == token.ADD || node.Op == token.SUB) && inertLiteralExpression(node.X)
	case *ast.CompositeLit:
		if !inertLiteralType(node.Type) {
			return false
		}
		for _, element := range node.Elts {
			if pair, ok := element.(*ast.KeyValueExpr); ok {
				if !inertLiteralExpression(pair.Key) || !inertLiteralExpression(pair.Value) {
					return false
				}
				continue
			}
			value, ok := element.(ast.Expr)
			if !ok || !inertLiteralExpression(value) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func inertLiteralType(expression ast.Expr) bool {
	switch node := expression.(type) {
	case *ast.Ident:
		switch node.Name {
		case "string", "bool", "byte", "rune", "int", "int8", "int16", "int32", "int64",
			"uint", "uint8", "uint16", "uint32", "uint64", "float32", "float64", "any":
			return true
		}
	case *ast.ArrayType:
		return node.Len == nil && inertLiteralType(node.Elt)
	case *ast.MapType:
		return inertLiteralType(node.Key) && inertLiteralType(node.Value)
	case *ast.InterfaceType:
		return node.Methods != nil && len(node.Methods.List) == 0
	}
	return false
}

func topLevelDeclaredVariables(code string) map[string]struct{} {
	declared := make(map[string]struct{})
	file, err := parser.ParseFile(token.NewFileSet(), "snippet.go", "package p\nfunc _(){\n"+code+"\n}\n", parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		return declared
	}
	function, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok || function.Body == nil {
		return declared
	}
	for _, statement := range function.Body.List {
		switch node := statement.(type) {
		case *ast.AssignStmt:
			if node.Tok != token.DEFINE {
				continue
			}
			for _, target := range node.Lhs {
				if ident, ok := target.(*ast.Ident); ok && ident.Name != "_" {
					declared[ident.Name] = struct{}{}
				}
			}
		case *ast.DeclStmt:
			declaration, ok := node.Decl.(*ast.GenDecl)
			if !ok || declaration.Tok != token.VAR {
				continue
			}
			for _, spec := range declaration.Specs {
				valueSpec, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for _, name := range valueSpec.Names {
					if name.Name != "_" {
						declared[name.Name] = struct{}{}
					}
				}
			}
		}
	}
	return declared
}

func stripVariableMarkers(output string) string {
	lines := strings.Split(output, "\n")
	kept := lines[:0]
	for _, line := range lines {
		if !strings.HasPrefix(line, "__RLM_VARS__") {
			kept = append(kept, line)
		}
	}
	return strings.Join(kept, "\n")
}

// extractVariables parses output to extract variable values.
func (e *ContainerExecutor) extractVariables(output string) {
	marker := "__RLM_VARS__" + e.finalTok + "__"
	idx := strings.LastIndex(output, marker)
	if idx == -1 {
		return
	}
	encoded := output[idx+len(marker):]
	if newline := strings.IndexByte(encoded, '\n'); newline >= 0 {
		encoded = encoded[:newline]
	}
	payload, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
	if err != nil {
		return
	}
	var vars map[string]any
	if err := json.Unmarshal(payload, &vars); err != nil {
		return
	}

	for k, v := range vars {
		literal, ok := v.(string)
		if !ok || !token.IsIdentifier(k) || !isInertGoLiteral(literal) {
			continue
		}
		e.variables[k] = literal
	}
}

func (e *ContainerExecutor) extractFinal(output, token string) {
	value, ok := findFinalMarker(output, token)
	if !ok {
		return
	}
	e.finalSet = true
	e.finalVal = value
}

// HasFinal reports whether executed code called FINAL or FINAL_VAR.
func (e *ContainerExecutor) HasFinal() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.finalSet
}

// Final returns the value supplied to FINAL or FINAL_VAR.
func (e *ContainerExecutor) Final() (string, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.finalSet {
		return "", false
	}
	return e.finalVal, true
}

// ClearFinal clears any previous FINAL/FINAL_VAR signal.
func (e *ContainerExecutor) ClearFinal() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.finalSet = false
	e.finalVal = ""
}

// LoadContext stores the context payload for injection into the container.
func (e *ContainerExecutor) LoadContext(payload any) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.contextData = payload
	if payload == nil {
		e.contextData = nil
	}
	return nil
}

// GetVariable retrieves a variable value.
func (e *ContainerExecutor) GetVariable(name string) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if v, ok := e.variables[name]; ok {
		return fmt.Sprintf("%v", v), nil
	}
	return "", fmt.Errorf("variable %q not found", name)
}

// GetLLMCalls returns and clears the recorded LLM calls.
func (e *ContainerExecutor) GetLLMCalls() []LLMCall {
	if e.ipcServer == nil {
		return nil
	}
	return e.ipcServer.GetCalls()
}

// GetLocals returns user-defined variables.
func (e *ContainerExecutor) GetLocals() map[string]any {
	e.mu.Lock()
	defer e.mu.Unlock()

	result := make(map[string]any)
	for k, v := range e.variables {
		result[k] = v
	}
	return result
}

// ContextInfo returns metadata about the loaded context.
func (e *ContainerExecutor) ContextInfo() string {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.contextData == nil {
		return "context not loaded"
	}

	switch v := e.contextData.(type) {
	case string:
		return fmt.Sprintf("type=string, len=%d", len(v))
	case []any:
		return fmt.Sprintf("type=array, len=%d", len(v))
	case map[string]any:
		return fmt.Sprintf("type=object, keys=%d", len(v))
	default:
		return fmt.Sprintf("type=%T", v)
	}
}

// Reset clears the execution state.
func (e *ContainerExecutor) Reset() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.variables = make(map[string]any)
	e.contextData = nil
	e.finalSet = false
	e.finalVal = ""
	e.finalTok = newFinalToken()

	if e.ipcServer != nil {
		e.ipcServer.ClearCalls()
	}

	return nil
}

// Close releases resources.
func (e *ContainerExecutor) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.ipcServer != nil {
		return e.ipcServer.Stop()
	}
	return nil
}

// Backend returns the backend type.
func (e *ContainerExecutor) Backend() Backend {
	return e.backend
}

// PullImage pulls the container image if not already present.
func (e *ContainerExecutor) PullImage(ctx context.Context) error {
	image := e.config.Image
	if image == "" {
		image = "golang:1.23-alpine"
	}

	cmd := exec.CommandContext(ctx, e.runtime, "pull", image)
	return cmd.Run()
}

// ImageExists checks if the container image exists locally.
func (e *ContainerExecutor) ImageExists() bool {
	image := e.config.Image
	if image == "" {
		image = "golang:1.23-alpine"
	}

	cmd := exec.Command(e.runtime, "image", "inspect", image)
	return cmd.Run() == nil
}

// RuntimeInfo returns information about the container runtime.
func (e *ContainerExecutor) RuntimeInfo() (string, error) {
	cmd := exec.Command(e.runtime, "--version")
	output, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}
