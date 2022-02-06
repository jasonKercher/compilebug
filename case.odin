//+private
package streamql

Case_State :: enum u8 {
	Static,
	Static_Cmp,
	Logic_Group,
	Value,
}

Expr_Case :: struct {
	values: [dynamic]Expression,
	tests: [dynamic]^Logic_Group,
	return_state_idx: i32,
	state: Case_State,
}
