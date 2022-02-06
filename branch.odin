//+private
package streamql

Branch_Type :: enum {
	If,
	Else_If,
	While,
}

/* Will probably be able to drop this... */
Branch_Status :: enum {
	Expecting_Else,
}

Branch :: struct {
	condition: Logic_Group,
	last_true_block_query: ^Query,
	status: bit_set[Branch_Status],
	type: Branch_Type,
	next_query_idx_ref: ^i32,
	scope: i32,
	else_scope: i32,
	parent_scope: i32,
	false_idx: i32,
	true_idx: i32,
}

branch_preop :: proc(b: ^Branch, q: ^Query) -> Result {
	return not_implemented()
}

branch_apply_process :: proc(q: ^Query) -> Result {
	return not_implemented()
}
