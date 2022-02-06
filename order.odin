//+private
package streamql

import "core:fmt"

Order :: struct {
	expressions: [dynamic]Expression,
	top_count: i64,
}

destroy_order :: proc(o: ^Order) {
	delete(o.expressions)
}

order_add_expression :: proc(o: ^Order, expr: ^Expression) -> ^Expression {
	append(&o.expressions, expr^)
	return &o.expressions[len(o.expressions) - 1]
}

order_preresolve :: proc(o: ^Order, sel: ^Select, sources: []Source) -> Result {
	/* First resolve ordinals:
	 * SELECT foo
	 * FROM t1
	 * ORDER BY 1
	 *
	 * Next Order by alias:
	 * SELECT num1 + num2 mysum
	 * FROM t1
	 * ORDER BY mysum
	 */

	sel := sel
	schema_preflight(&sel.schema)

	remove_list : [dynamic]int
	
	for e, i in &o.expressions {
		item := Schema_Item { loc = -1 }
		#partial switch v in &e.data {
		case Expr_Reference:
			item = schema_get_item(&sel.schema, e.alias) or_return
		case Expr_Full_Record:
			item = schema_get_item(&sel.schema, e.alias) or_return
		case Expr_Column_Name:
			item = schema_get_item(&sel.schema, e.alias) or_return
		case Expr_Constant:
			ordinal := expression_get_int(&e) or_return
			if ordinal <= 0 || int(ordinal) > len(sel.schema.layout) {
				fmt.eprintf("ordinal `%d' out of range\n", ordinal)
				return .Error
			}
			item = sel.schema.layout[ordinal - 1]
		}

		if item.loc == -1 {
			continue
		}

		select_expr := &sel.expressions[item.loc]
		if _, is_const := select_expr.data.(Expr_Constant); is_const {
			append(&remove_list, i)
			continue
		}

		e.data = Expr_Reference(select_expr)
	}

	/* remove any constant references */
	num_removed: int
	for idx in remove_list {
		ordered_remove(&o.expressions, idx - num_removed)
		num_removed += 1
	}

	return .Ok
}
