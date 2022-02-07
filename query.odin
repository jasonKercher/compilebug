//+private
package streamql

import "util"
import "core:os"
import "core:fmt"
import "core:strings"
import "core:math/bits"

Query :: struct {
	operation: Select,
	plan: Plan,
	unions: [dynamic]^Query,
	subquery_exprs: [dynamic]^Query,
	var_source_vars: [dynamic]i32,
	var_sources: [dynamic]i32,
	var_expr_vars: [dynamic]i32,
	into_table_name: string,
	preview_text: string,
	top_count: i64,
	next_idx_ref: ^u32,
	next_idx: u32,
	idx: u32,
	into_table_var: i32,
	union_id: i32,
	sub_id: i16,
	query_total: i16,
}

