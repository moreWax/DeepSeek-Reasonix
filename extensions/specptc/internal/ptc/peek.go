package ptc

import (
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strconv"
	"strings"
)

const defaultMaxUnroll = 64

var errUnresolvable = errors.New("ptc: expression is not safely resolvable")

// ToolPolicy declares a function visible to generated RLM code. Speculation is
// always explicit; Deterministic permits one execution to satisfy repeated
// identical calls while the default preserves independent samples.
type ToolPolicy struct {
	Deterministic bool
	// ExecuteAs maps a generated convenience function (for example
	// QueryAsync) onto the canonical executor identity used at claim time.
	ExecuteAs string
	// Canonicalize converts generated arguments to the exact values the real
	// runtime passes to the executor. Returning false makes the call ineligible.
	Canonicalize func([]any) ([]any, bool)
}

// CallPlan is one safely resolved call found in a partial Go REPL program.
type CallPlan struct {
	ID            string
	Name          string
	Values        []any
	Arguments     json.RawMessage
	Deterministic bool
}

// Planner resolves calls from partial streamed Go using only copied, inert
// values. Unknown values taint dependent statements but do not prevent later
// independent calls from being planned.
type Planner struct {
	Tools     map[string]ToolPolicy
	MaxUnroll int
	// Results contains completed speculative values keyed by CallPlan.ID. It
	// enables a later dependent call to become eligible without executing code.
	Results map[string]any
}

// Plan repairs an open streamed tail, resolves reachable tool calls, and
// unrolls bounded range loops. It returns no plans when parsing or safe
// evaluation is uncertain; ordinary real execution remains authoritative.
func (p Planner) Plan(tail string, env map[string]any) []CallPlan {
	plans, _ := p.PlanChecked(tail, env)
	return plans
}

// PlanChecked is Plan plus a validity bit. false means the streamed suffix is
// temporarily unparsable, so callers must preserve earlier speculative bets
// until a later delta makes the program valid or disproves them.
func (p Planner) PlanChecked(tail string, env map[string]any) ([]CallPlan, bool) {
	plans, _, valid := p.PlanCheckedState(tail, env)
	return plans, valid
}

// PlanCheckedState also returns a copied shadow namespace after evaluating the
// complete safe subset. Callers can carry that namespace across REPL blocks.
func (p Planner) PlanCheckedState(tail string, env map[string]any) ([]CallPlan, map[string]any, bool) {
	source, ok := repairTail(tail)
	if !ok {
		return nil, nil, false
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "peek.go", "package p\nfunc _(){\n"+source+"\n}\n", parser.AllErrors)
	if err != nil || len(file.Decls) != 1 {
		return nil, nil, false
	}
	decl, ok := file.Decls[0].(*ast.FuncDecl)
	if !ok || decl.Body == nil {
		return nil, nil, false
	}
	state := make(map[string]any, len(env))
	for name, value := range env {
		state[name] = cloneInert(value)
	}
	walker := planWalker{planner: p.normalized(), fset: fset, env: state}
	walker.block(decl.Body.List, nil)
	return walker.plans, cloneEnvironment(walker.env), true
}

func (p Planner) normalized() Planner {
	if p.MaxUnroll <= 0 {
		p.MaxUnroll = defaultMaxUnroll
	}
	return p
}

type unresolvedValue struct{}

type planWalker struct {
	planner Planner
	fset    *token.FileSet
	env     map[string]any
	scopes  []map[string]scopeBinding
	plans   []CallPlan
}

type scopeBinding struct {
	value   any
	existed bool
}

type controlFlow uint8

const (
	flowNormal controlFlow = iota
	flowBreak
	flowContinue
	flowReturn
	flowStop
)

func (w *planWalker) block(statements []ast.Stmt, path []int) controlFlow {
	for index, statement := range statements {
		if flow := w.statement(statement, appendPath(path, index)); flow != flowNormal {
			return flow
		}
	}
	return flowNormal
}

func (w *planWalker) statement(statement ast.Stmt, path []int) controlFlow {
	switch node := statement.(type) {
	case *ast.AssignStmt:
		if len(node.Lhs) != len(node.Rhs) {
			for _, expr := range node.Rhs {
				w.collect(expr, path)
			}
			w.taintTargets(node.Lhs)
			return flowNormal
		}
		values := make([]any, len(node.Rhs))
		for i, expr := range node.Rhs {
			values[i] = unresolvedValue{}
			before := len(w.plans)
			w.collect(expr, path)
			if len(w.plans) > before {
				if value, ok := w.completedExpression(expr, w.plans[before:]); ok {
					values[i] = value
				}
				continue
			}
			if value, err := safeEval(expr, w.env); err == nil {
				values[i] = value
			}
		}
		for i, target := range node.Lhs {
			w.bind(target, values[i], node.Tok == token.DEFINE)
		}
	case *ast.DeclStmt:
		decl, ok := node.Decl.(*ast.GenDecl)
		if !ok || decl.Tok != token.VAR {
			return flowNormal
		}
		for _, spec := range decl.Specs {
			valueSpec, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, name := range valueSpec.Names {
				value := any(unresolvedValue{})
				if i < len(valueSpec.Values) {
					expr := valueSpec.Values[i]
					before := len(w.plans)
					w.collect(expr, path)
					if len(w.plans) > before {
						if resolved, ready := w.completedExpression(expr, w.plans[before:]); ready {
							value = resolved
						}
					} else if resolved, evalErr := safeEval(expr, w.env); evalErr == nil {
						value = resolved
					}
				}
				w.bind(name, value, true)
			}
		}
	case *ast.ExprStmt:
		w.collect(node.X, path)
	case *ast.IfStmt:
		w.pushScope()
		defer w.popScope()
		if node.Init != nil {
			if flow := w.statement(node.Init, appendPath(path, -1)); flow != flowNormal {
				return flow
			}
		}
		condition, err := safeEval(node.Cond, w.env)
		if err != nil {
			return flowStop
		}
		take, ok := condition.(bool)
		if !ok {
			return flowStop
		}
		if take {
			return w.scopedBlock(node.Body.List, appendPath(path, 1))
		}
		if node.Else != nil {
			return w.scopedStatement(node.Else, appendPath(path, 0))
		}
	case *ast.BlockStmt:
		return w.scopedBlock(node.List, path)
	case *ast.RangeStmt:
		iterable, err := safeEval(node.X, w.env)
		if err != nil {
			return flowStop
		}
		values, ok := rangeValues(iterable, w.planner.MaxUnroll)
		if !ok {
			return flowStop
		}
		w.pushScope()
		defer w.popScope()
		declare := node.Tok == token.DEFINE
		for index, value := range values {
			if node.Key != nil {
				w.bind(node.Key, index, declare)
			}
			if node.Value != nil {
				w.bind(node.Value, value, declare)
			}
			switch flow := w.scopedBlock(node.Body.List, appendPath(path, index)); flow {
			case flowReturn, flowStop:
				return flow
			case flowBreak:
				return flowNormal
			case flowContinue, flowNormal:
				continue
			}
		}
	case *ast.BranchStmt:
		if node.Label != nil {
			return flowStop
		}
		switch node.Tok {
		case token.BREAK:
			return flowBreak
		case token.CONTINUE:
			return flowContinue
		case token.RETURN:
			return flowReturn
		}
	case *ast.ForStmt, *ast.SwitchStmt, *ast.TypeSwitchStmt, *ast.SelectStmt,
		*ast.GoStmt, *ast.DeferStmt:
		return flowStop
	case *ast.LabeledStmt:
		// Resolving labeled branch targets requires a full control-flow graph.
		// Stop conservatively rather than dispatching unreachable calls.
		return flowStop
	case *ast.ReturnStmt:
		for _, result := range node.Results {
			w.collect(result, path)
		}
		return flowReturn
	case *ast.IncDecStmt:
		ident, ok := node.X.(*ast.Ident)
		if !ok {
			return flowNormal
		}
		value, ok := asInt(w.env[ident.Name])
		if !ok {
			w.env[ident.Name] = unresolvedValue{}
			return flowNormal
		}
		if node.Tok == token.INC {
			value++
		} else {
			value--
		}
		w.env[ident.Name] = value
	}
	return flowNormal
}

func (w *planWalker) completedExpression(expr ast.Expr, plans []CallPlan) (any, bool) {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return nil, false
	}
	name := callName(call.Fun)
	if strings.Contains(name, "Async") {
		return nil, false
	}
	if _, speculative := w.planner.Tools[name]; !speculative {
		return nil, false
	}
	values := make([]any, len(plans))
	for i, plan := range plans {
		value, exists := w.planner.Results[plan.ID]
		if !exists {
			return nil, false
		}
		values[i] = cloneInert(value)
	}
	if strings.HasPrefix(name, "QueryBatched") {
		return values, true
	}
	if len(values) != 1 {
		return nil, false
	}
	return values[0], true
}

func (w *planWalker) collect(expr ast.Expr, path []int) {
	ast.Inspect(expr, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := callName(call.Fun)
		policy, speculative := w.planner.Tools[name]
		if !speculative {
			return true
		}
		if name == "QueryBatched" || name == "QueryBatchedRaw" || name == "QueryBatchedAsync" {
			w.collectBatch(call, policy, path)
			return false
		}
		values := make([]any, len(call.Args))
		for i, argument := range call.Args {
			value, err := safeEval(argument, w.env)
			if err != nil {
				return false
			}
			values[i] = value
		}
		w.appendPlan(call, name, values, policy, path)
		return false
	})
}

func (w *planWalker) collectBatch(call *ast.CallExpr, policy ToolPolicy, path []int) {
	if len(call.Args) != 1 {
		return
	}
	value, err := safeEval(call.Args[0], w.env)
	if err != nil {
		return
	}
	items, ok := rangeValues(value, w.planner.MaxUnroll)
	if !ok {
		return
	}
	name := "Query"
	if strings.Contains(callName(call.Fun), "Raw") {
		name = "QueryRaw"
	}
	if declared, exists := w.planner.Tools[name]; exists {
		policy = declared
	}
	for i, item := range items {
		w.appendPlan(call, name, []any{item}, policy, appendPath(path, i))
	}
}

func (w *planWalker) appendPlan(call *ast.CallExpr, name string, values []any, policy ToolPolicy, path []int) {
	if policy.Canonicalize != nil {
		var ok bool
		values, ok = policy.Canonicalize(values)
		if !ok {
			return
		}
	}
	if policy.ExecuteAs != "" {
		name = policy.ExecuteAs
	}
	arguments, err := json.Marshal(struct {
		Args []any `json:"args"`
	}{Args: values})
	if err != nil {
		return
	}
	position := w.fset.Position(call.Pos())
	w.plans = append(w.plans, CallPlan{
		ID: fmt.Sprintf("peek:%d:%s", position.Offset, pathKey(path)), Name: name,
		Values: values, Arguments: arguments, Deterministic: policy.Deterministic,
	})
}

func (w *planWalker) taintTargets(targets []ast.Expr) {
	for _, target := range targets {
		w.bind(target, unresolvedValue{}, false)
	}
}

func (w *planWalker) scopedBlock(statements []ast.Stmt, path []int) controlFlow {
	w.pushScope()
	defer w.popScope()
	return w.block(statements, path)
}

func (w *planWalker) scopedStatement(statement ast.Stmt, path []int) controlFlow {
	w.pushScope()
	defer w.popScope()
	return w.statement(statement, path)
}

func (w *planWalker) pushScope() {
	w.scopes = append(w.scopes, make(map[string]scopeBinding))
}

func (w *planWalker) popScope() {
	frame := w.scopes[len(w.scopes)-1]
	w.scopes = w.scopes[:len(w.scopes)-1]
	for name, prior := range frame {
		if prior.existed {
			w.env[name] = prior.value
		} else {
			delete(w.env, name)
		}
	}
}

func (w *planWalker) bind(target ast.Expr, value any, declare bool) {
	switch node := target.(type) {
	case *ast.Ident:
		if node.Name != "_" {
			w.recordDeclaration(node.Name, declare)
			w.env[node.Name] = value
		}
	case *ast.IndexExpr:
		// Mutating copied containers is unnecessary for safe planning. Mark the
		// base name unknown so later calls cannot observe stale data.
		if ident, ok := node.X.(*ast.Ident); ok {
			w.env[ident.Name] = unresolvedValue{}
		}
	}
}

func (w *planWalker) recordDeclaration(name string, declare bool) {
	if !declare || len(w.scopes) == 0 {
		return
	}
	frame := w.scopes[len(w.scopes)-1]
	if _, declared := frame[name]; declared {
		return
	}
	prior, existed := w.env[name]
	frame[name] = scopeBinding{value: prior, existed: existed}
}

func safeEval(expr ast.Expr, env map[string]any) (any, error) {
	switch node := expr.(type) {
	case *ast.BasicLit:
		switch node.Kind {
		case token.STRING, token.CHAR:
			return strconv.Unquote(node.Value)
		case token.INT:
			return strconv.Atoi(node.Value)
		case token.FLOAT:
			return strconv.ParseFloat(node.Value, 64)
		}
	case *ast.Ident:
		switch node.Name {
		case "true":
			return true, nil
		case "false":
			return false, nil
		case "nil":
			return nil, nil
		}
		value, ok := env[node.Name]
		if !ok {
			return nil, errUnresolvable
		}
		if _, unknown := value.(unresolvedValue); unknown {
			return nil, errUnresolvable
		}
		return value, nil
	case *ast.ParenExpr:
		return safeEval(node.X, env)
	case *ast.UnaryExpr:
		value, err := safeEval(node.X, env)
		if err != nil {
			return nil, err
		}
		switch node.Op {
		case token.NOT:
			boolean, ok := value.(bool)
			if ok {
				return !boolean, nil
			}
		case token.SUB:
			integer, ok := asInt(value)
			if ok {
				return -integer, nil
			}
		}
	case *ast.BinaryExpr:
		left, err := safeEval(node.X, env)
		if err != nil {
			return nil, err
		}
		right, err := safeEval(node.Y, env)
		if err != nil {
			return nil, err
		}
		return evalBinary(node.Op, left, right)
	case *ast.CompositeLit:
		values := make([]any, 0, len(node.Elts))
		for _, element := range node.Elts {
			expr, ok := element.(ast.Expr)
			if !ok {
				return nil, errUnresolvable
			}
			value, err := safeEval(expr, env)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		return values, nil
	case *ast.IndexExpr:
		container, err := safeEval(node.X, env)
		if err != nil {
			return nil, err
		}
		index, err := safeEval(node.Index, env)
		if err != nil {
			return nil, err
		}
		return indexValue(container, index)
	case *ast.SliceExpr:
		container, err := safeEval(node.X, env)
		if err != nil {
			return nil, err
		}
		return sliceValue(container, node, env)
	case *ast.CallExpr:
		return evalSafeCall(node, env)
	}
	return nil, errUnresolvable
}

func evalBinary(operator token.Token, left, right any) (any, error) {
	if operator == token.ADD {
		if a, ok := left.(string); ok {
			if b, ok := right.(string); ok {
				return a + b, nil
			}
		}
		if a, ok := asInt(left); ok {
			if b, ok := asInt(right); ok {
				return a + b, nil
			}
		}
	}
	if operator == token.EQL || operator == token.NEQ {
		equal := reflect.DeepEqual(left, right)
		if operator == token.NEQ {
			equal = !equal
		}
		return equal, nil
	}
	if a, ok := asInt(left); ok {
		if b, ok := asInt(right); ok {
			switch operator {
			case token.SUB:
				return a - b, nil
			case token.MUL:
				return a * b, nil
			case token.QUO:
				if b != 0 {
					return a / b, nil
				}
			case token.LSS:
				return a < b, nil
			case token.LEQ:
				return a <= b, nil
			case token.GTR:
				return a > b, nil
			case token.GEQ:
				return a >= b, nil
			}
		}
	}
	return nil, errUnresolvable
}

func evalSafeCall(call *ast.CallExpr, env map[string]any) (any, error) {
	name := callName(call.Fun)
	values := make([]any, len(call.Args))
	for i, argument := range call.Args {
		value, err := safeEval(argument, env)
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	switch name {
	case "len":
		if len(values) == 1 {
			value := reflect.ValueOf(values[0])
			if value.IsValid() && (value.Kind() == reflect.Array || value.Kind() == reflect.Slice || value.Kind() == reflect.Map || value.Kind() == reflect.String) {
				return value.Len(), nil
			}
		}
	case "string", "fmt.Sprint", "fmt.Sprintf":
		if name == "fmt.Sprintf" && len(values) > 0 {
			format, ok := values[0].(string)
			if ok {
				return fmt.Sprintf(format, values[1:]...), nil
			}
		}
		if len(values) == 1 {
			return fmt.Sprint(values[0]), nil
		}
	case "strings.Join":
		if len(values) == 2 {
			separator, ok := values[1].(string)
			if !ok {
				return nil, errUnresolvable
			}
			items, ok := toStrings(values[0])
			if ok {
				return strings.Join(items, separator), nil
			}
		}
	}
	return nil, errUnresolvable
}

func callName(expr ast.Expr) string {
	switch node := expr.(type) {
	case *ast.Ident:
		return node.Name
	case *ast.SelectorExpr:
		prefix := callName(node.X)
		if prefix == "" {
			return node.Sel.Name
		}
		return prefix + "." + node.Sel.Name
	}
	return ""
}

func rangeValues(value any, limit int) ([]any, bool) {
	reflected := reflect.ValueOf(value)
	if !reflected.IsValid() || (reflected.Kind() != reflect.Array && reflected.Kind() != reflect.Slice) || reflected.Len() > limit {
		return nil, false
	}
	out := make([]any, reflected.Len())
	for i := range reflected.Len() {
		out[i] = reflected.Index(i).Interface()
	}
	return out, true
}

func indexValue(container, index any) (any, error) {
	position, ok := asInt(index)
	if !ok || position < 0 {
		return nil, errUnresolvable
	}
	value := reflect.ValueOf(container)
	if !value.IsValid() || (value.Kind() != reflect.Array && value.Kind() != reflect.Slice && value.Kind() != reflect.String) || position >= value.Len() {
		return nil, errUnresolvable
	}
	if value.Kind() == reflect.String {
		return string(value.String()[position]), nil
	}
	return value.Index(position).Interface(), nil
}

func sliceValue(container any, slice *ast.SliceExpr, env map[string]any) (any, error) {
	value := reflect.ValueOf(container)
	if !value.IsValid() || (value.Kind() != reflect.Array && value.Kind() != reflect.Slice && value.Kind() != reflect.String) {
		return nil, errUnresolvable
	}
	low, high := 0, value.Len()
	var err error
	if slice.Low != nil {
		var resolved any
		resolved, err = safeEval(slice.Low, env)
		low, _ = asInt(resolved)
	}
	if err == nil && slice.High != nil {
		var resolved any
		resolved, err = safeEval(slice.High, env)
		high, _ = asInt(resolved)
	}
	if err != nil || low < 0 || high < low || high > value.Len() {
		return nil, errUnresolvable
	}
	return value.Slice(low, high).Interface(), nil
}

func asInt(value any) (int, bool) {
	switch number := value.(type) {
	case int:
		return number, true
	case int8:
		return int(number), true
	case int16:
		return int(number), true
	case int32:
		return int(number), true
	case int64:
		return int(number), true
	case uint:
		return int(number), true
	case uint8:
		return int(number), true
	case uint16:
		return int(number), true
	case uint32:
		return int(number), true
	case uint64:
		if uint64(int(number)) == number {
			return int(number), true
		}
	}
	return 0, false
}

func toStrings(value any) ([]string, bool) {
	items, ok := rangeValues(value, int(^uint(0)>>1))
	if !ok {
		return nil, false
	}
	out := make([]string, len(items))
	for i, item := range items {
		text, ok := item.(string)
		if !ok {
			return nil, false
		}
		out[i] = text
	}
	return out, true
}

func repairTail(source string) (string, bool) {
	if strings.TrimSpace(source) == "" {
		return "", false
	}
	paren, bracket, brace, open := lexicalBalance(source)
	if open || paren < 0 || bracket < 0 || brace < 0 {
		return "", false
	}
	var repaired strings.Builder
	repaired.WriteString(source)
	for range paren {
		repaired.WriteByte(')')
	}
	for range bracket {
		repaired.WriteByte(']')
	}
	for range brace {
		repaired.WriteString("\n}")
	}
	return repaired.String(), true
}

func lexicalBalance(source string) (paren, bracket, brace int, open bool) {
	var quote rune
	escaped, lineComment, blockComment := false, false, false
	runes := []rune(source)
	for i, r := range runes {
		if lineComment {
			if r == '\n' {
				lineComment = false
			}
			continue
		}
		if blockComment {
			if r == '*' && i+1 < len(runes) && runes[i+1] == '/' {
				blockComment = false
			}
			continue
		}
		if quote != 0 {
			if quote == '`' {
				if r == '`' {
					quote = 0
				}
				continue
			}
			if escaped {
				escaped = false
				continue
			}
			if r == '\\' {
				escaped = true
				continue
			}
			if r == quote {
				quote = 0
			}
			continue
		}
		if r == '/' && i+1 < len(runes) {
			if runes[i+1] == '/' {
				lineComment = true
				continue
			}
			if runes[i+1] == '*' {
				blockComment = true
				continue
			}
		}
		switch r {
		case '\'', '"', '`':
			quote = r
		case '(':
			paren++
		case ')':
			paren--
		case '[':
			bracket++
		case ']':
			bracket--
		case '{':
			brace++
		case '}':
			brace--
		}
	}
	return paren, bracket, brace, quote != 0 || blockComment
}

func appendPath(path []int, value int) []int {
	out := make([]int, len(path)+1)
	copy(out, path)
	out[len(path)] = value
	return out
}

func pathKey(path []int) string {
	parts := make([]string, len(path))
	for i, value := range path {
		parts[i] = strconv.Itoa(value)
	}
	return strings.Join(parts, ".")
}

func cloneEnvironment(env map[string]any) map[string]any {
	cloned := make(map[string]any, len(env))
	for name, value := range env {
		cloned[name] = cloneInert(value)
	}
	return cloned
}

func cloneInert(value any) any {
	data, err := json.Marshal(value)
	if err != nil {
		return unresolvedValue{}
	}
	var cloned any
	if err := json.Unmarshal(data, &cloned); err != nil {
		return unresolvedValue{}
	}
	return cloned
}
