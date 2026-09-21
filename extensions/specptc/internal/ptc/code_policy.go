package ptc

import (
	"context"
	"errors"
	"fmt"
	"go/scanner"
	"go/token"

	rlmcore "github.com/XiaoConstantine/rlm-go/pkg/core"
	"github.com/XiaoConstantine/rlm-go/pkg/parsing"
	rlmgo "github.com/XiaoConstantine/rlm-go/pkg/rlm"
)

// constrainedRootClient prevents streamed model statements from importing new
// capabilities or reaching private sandbox IPC primitives. Query APIs remain
// the sole model egress path in both container and local execution modes.
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
	var lexer scanner.Scanner
	tokenFile := token.NewFileSet().AddFile("generated.go", -1, len(code))
	lexer.Init(tokenFile, []byte(code), nil, scanner.ScanComments)
	for {
		_, kind, literal := lexer.Scan()
		switch kind {
		case token.IMPORT:
			return errors.New("ptc: generated code may not import packages")
		case token.IDENT:
			if _, denied := forbiddenGeneratedIdentifiers[literal]; denied {
				return fmt.Errorf("ptc: generated code references forbidden sandbox capability %q", literal)
			}
		case token.EOF:
			return nil
		}
	}
}
