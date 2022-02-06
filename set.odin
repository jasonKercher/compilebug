//+private
package streamql

Set :: struct {
	init_expr: Expression,
}

set_preop :: proc(s: ^Set, q: ^Query) -> Result {
	return not_implemented()
}

set_set_init_expression :: proc(s: ^Set, expr: ^Expression) -> ^Expression {
	s.init_expr = expr^
	return &s.init_expr
}

set_apply_process :: proc(q: ^Query) -> Result {
	return not_implemented()
}
