//+private
package streamql

Delete :: struct {
	schema: Schema,
	top_count: i64,
	src_idx: i32,
}

delete_reset :: proc(d: ^Delete) -> Result {
	return not_implemented()
}

delete_preop :: proc(d: ^Delete, q: ^Query) -> Result {
	return not_implemented()
}

delete_apply_process :: proc(q: ^Query) -> Result {
	return not_implemented()
}
