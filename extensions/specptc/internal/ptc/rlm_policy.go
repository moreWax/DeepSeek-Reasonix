package ptc

import "fmt"

const rlmQuerySuffix = "IMPORTANT: Provide a direct, concise answer. Do not explain your reasoning unless specifically asked."

// RLMToolPolicies returns the canonical mapping between rlm-go's generated
// convenience functions and the underlying sub-model Query call. This makes
// speculative and authoritative keys byte-identical.
func RLMToolPolicies(contextValue string) map[string]ToolPolicy {
	withContext := func(values []any) ([]any, bool) {
		if len(values) != 1 {
			return nil, false
		}
		prompt, ok := values[0].(string)
		if !ok {
			return nil, false
		}
		return []any{RLMQueryPrompt(contextValue, prompt)}, true
	}
	raw := func(values []any) ([]any, bool) {
		if len(values) != 1 {
			return nil, false
		}
		_, ok := values[0].(string)
		return values, ok
	}
	providedContext := func(values []any) ([]any, bool) {
		if len(values) != 2 {
			return nil, false
		}
		contextSlice, contextOK := values[0].(string)
		prompt, promptOK := values[1].(string)
		if !contextOK || !promptOK {
			return nil, false
		}
		return []any{RLMQueryPrompt(contextSlice, prompt)}, true
	}
	return map[string]ToolPolicy{
		"Query":             {Canonicalize: withContext},
		"QueryRaw":          {ExecuteAs: "Query", Canonicalize: raw},
		"QueryWith":         {ExecuteAs: "Query", Canonicalize: providedContext},
		"QueryAsync":        {ExecuteAs: "Query", Canonicalize: withContext},
		"QueryBatched":      {Canonicalize: withContext},
		"QueryBatchedRaw":   {ExecuteAs: "Query", Canonicalize: raw},
		"QueryBatchedAsync": {Canonicalize: withContext},
	}
}

// RLMQueryPrompt matches rlm-go's REPL prompt construction exactly.
func RLMQueryPrompt(contextValue, prompt string) string {
	if contextValue == "" {
		return prompt
	}
	return fmt.Sprintf("Context data:\n%s\n\nTask: %s\n\n%s", contextValue, prompt, rlmQuerySuffix)
}
