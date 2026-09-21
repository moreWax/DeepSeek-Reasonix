package ptc

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"

	rlmcore "github.com/XiaoConstantine/rlm-go/pkg/core"
	"github.com/XiaoConstantine/rlm-go/pkg/parsing"
	rlmgo "github.com/XiaoConstantine/rlm-go/pkg/rlm"
)

// constrainedRootClient prevents streamed model statements from reaching the
// networking primitives imported by rlm-go's container IPC prelude. Query APIs
// remain the sole egress path and are fenced by the turn-local execution ID
// enforced by rlm-go's IPC server.
type constrainedRootClient struct {
	client rlmgo.LLMClient
}

func (c constrainedRootClient) Complete(ctx context.Context, messages []rlmcore.Message) (rlmcore.LLMResponse, error) {
	response, err := c.client.Complete(ctx, messages)
	if err != nil {
		return response, err
	}
	if err := validateGeneratedCode(response.Content); err != nil {
		return rlmcore.LLMResponse{}, err
	}
	return response, nil
}

func (c constrainedRootClient) CompleteStream(
	ctx context.Context,
	messages []rlmcore.Message,
	handler rlmgo.StreamHandler,
) (rlmcore.LLMResponse, error) {
	streaming, ok := c.client.(rlmgo.StreamingLLMClient)
	if !ok {
		return rlmcore.LLMResponse{}, errors.New("ptc: root model does not support streaming")
	}
	response, err := streaming.CompleteStream(ctx, messages, handler)
	if err != nil {
		return response, err
	}
	if err := validateGeneratedCode(response.Content); err != nil {
		return rlmcore.LLMResponse{}, err
	}
	return response, nil
}

var forbiddenGeneratedIdentifiers = map[string]struct{}{
	// The generated container program imports net only for its private IPC
	// connection. Model statements must not create arbitrary connections.
	"net": {},
	// Private prelude state, protocol types, and helpers must not be callable
	// from model statements. Public Query/context APIs are intentionally absent.
	"ipcConn": {}, "ipcEncoder": {}, "ipcReader": {}, "ipcMu": {},
	"ipcCounter": {}, "ipcNetwork": {}, "ipcAddr": {}, "ipcExecutionID": {},
	"messageType": {}, "messageQuery": {}, "messageQueryBatched": {},
	"messageQueryBlocked": {}, "messageReserveQuery": {}, "messageResolveQuery": {},
	"messageResponse": {}, "messageError": {},
	"tokenUsage": {}, "ipcMessage": {}, "nextID": {}, "readIPCLine": {},
	"buildPromptWithContext": {}, "buildPromptWithProvidedContext": {},
	"fullContextQueryBlocked": {}, "contextChunks": {}, "runeByteOffsets": {},
	"contextSearchTerms": {}, "queryRaw": {}, "queryBatchedRaw": {},
	"reserveAsyncQuery": {}, "resolveAsyncQuery": {}, "reserveAsync": {},
	"recordBlockedQuery": {}, "recordBlockedQueries": {},
	"asyncQueryResult": {}, "asyncMu": {}, "asyncCounter": {}, "asyncQueries": {},
	"__rlmState": {}, "__rlmEncoded": {}, "__rlmPayload": {},
	"emitRLMState": {},
}

func validateGeneratedCode(response string) error {
	for _, block := range parsing.FindCodeBlocks(response) {
		if err := validateGeneratedBlock(block); err != nil {
			return err
		}
	}
	return nil
}

func validateGeneratedBlock(code string) error {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "generated.go", "package p\nfunc _(){\n"+code+"\n}\n", parser.AllErrors)
	if err != nil {
		// rlm-go will surface ordinary syntax errors as REPL feedback. Only a
		// successfully parsed forbidden reference is a policy violation.
		return nil
	}
	var forbidden string
	ast.Inspect(file, func(node ast.Node) bool {
		ident, ok := node.(*ast.Ident)
		if !ok {
			return true
		}
		if _, denied := forbiddenGeneratedIdentifiers[ident.Name]; denied {
			forbidden = ident.Name
			return false
		}
		return true
	})
	if forbidden != "" {
		return fmt.Errorf("ptc: generated code references forbidden sandbox capability %q", strings.TrimSpace(forbidden))
	}
	return nil
}
