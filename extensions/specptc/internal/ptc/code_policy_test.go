package ptc

import (
	"strings"
	"testing"
)

func TestGeneratedCodePolicyAllowsRLMQueries(t *testing.T) {
	response := "```go\nanswer := Query(\"summarize\")\nFINAL(answer)\n```"
	if err := validateGeneratedCode(response); err != nil {
		t.Fatal(err)
	}
}

func TestGeneratedCodePolicyRejectsDirectNetworkAndIPC(t *testing.T) {
	cases := []string{
		"```go\nconn, _ := net.Dial(\"tcp\", \"example.com:80\")\n_ = conn\n```",
		"```go\n_, _ = ipcConn.Write([]byte(\"probe\"))\n```",
		"```go\nrecordBlockedQueries([]string{\"x\"}, []string{\"y\"})\n```",
		"```go\nreservation := reserveAsyncQuery(\"bypass\")\n_ = reservation\n```",
	}
	for _, response := range cases {
		err := validateGeneratedCode(response)
		if err == nil || !strings.Contains(err.Error(), "forbidden sandbox capability") {
			t.Fatalf("validateGeneratedCode(%q) error = %v", response, err)
		}
	}
}

func TestGeneratedCodePolicyRejectsTopLevelDeclarationBypass(t *testing.T) {
	response := "```go\nfunc exploit() { _, _ = ipcConn.Write([]byte(\"probe\")) }\nexploit()\n```"
	err := validateGeneratedCode(response)
	if err == nil || !strings.Contains(err.Error(), "forbidden sandbox capability") {
		t.Fatalf("top-level declaration error = %v", err)
	}
}

func TestGeneratedCodePolicyRejectsImportsEvenWhenBlockIsNotAFunctionBody(t *testing.T) {
	response := "```go\nimport network \"net/http\"\nresponse, _ := network.Get(\"https://example.com\")\n_ = response\n```"
	err := validateGeneratedCode(response)
	if err == nil || !strings.Contains(err.Error(), "may not import") {
		t.Fatalf("import error = %v", err)
	}
}

func TestGeneratedCodePolicyIgnoresIdentifiersInStringsAndComments(t *testing.T) {
	response := "```go\n// net.Dial is forbidden\ntext := \"ipcConn\"\nprintln(text)\n```"
	if err := validateGeneratedCode(response); err != nil {
		t.Fatal(err)
	}
}
