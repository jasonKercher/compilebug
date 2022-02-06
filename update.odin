//+private
package streamql

import "core:fmt"

Update :: struct {
	schema: Schema,
	columns: [dynamic]Expression,
	values: [dynamic]Expression,
	top_count: i64,
	src_idx: i32,
}

update_reset :: proc(u: ^Update) -> Result {
	return not_implemented()
}

update_preop :: proc(u: ^Update, q: ^Query) -> Result {
	return not_implemented()
}

update_add_expression :: proc(u: ^Update, expr: ^Expression) -> (^Expression, Result) {
	if len(u.values) == len(u.columns) {
		_, ok_col := expr.data.(Expr_Column_Name)
		_, ok_var := expr.data.(Expr_Variable)
		if !ok_col && !ok_var {
			fmt.eprintln("unexpected expression as update element")
			return nil, .Error
		}
		append(&u.columns, expr^)
		return &u.columns[len(u.columns) - 1], .Ok
	}
	append(&u.values, expr^)
	return &u.values[len(u.values) - 1], .Ok
}

update_apply_process :: proc(q: ^Query) -> Result {
	return not_implemented()
}
