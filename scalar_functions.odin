//+private
package streamql

import "core:math/bits"
import "core:fmt"

/** operations **/

overflow_safe_add_i :: proc(n0, n1: i64) -> (i64, Result) {
	if (n0 > 0 && n1 > 0 && n0 + n1 < 0) || (n0 < 0 && n1 < 0 && n0 + n1 >= 0) {
		fmt.eprintf("Arithmetic overflow `%d + %d'\n", n0, n1)
		return 0, .Error
	}
	return n0 + n1, .Ok
}

overflow_safe_minus_i :: proc(n0, n1: i64) -> (i64, Result) {
	if (n0 > 0 && n1 < 0 && n0 - n1 < 0) || (n0 < 0 && n1 > 0 && n0 - n1 >= 0) {
		fmt.eprintf("Arithmetic overflow `%d - %d'\n", n0, n1)
		return 0, .Error
	}
	return n0 - n1, .Ok
}

overflow_safe_mult_i :: proc(n0, n1: i64) -> (i64, Result) {
	if n0 == 0 || n1 == 0 {
		return 0, .Ok
	}

	if (n0 * n1) / n0 != n1 {
		fmt.eprintf("Arithmetic overflow `%d * %d'\n", n0, n1)
		return 0, .Error
	}

	return n0 * n1, .Ok
}

sql_op_plus_i :: proc(fn: ^Expr_Function, data: ^Data, recs: ^Record = nil) -> Result {
	n0 := expression_get_int(&fn.args[0], recs) or_return
	n1 := expression_get_int(&fn.args[1], recs) or_return
	data^ = overflow_safe_add_i(n0, n1) or_return
	return .Ok
}

sql_op_plus_f :: proc(fn: ^Expr_Function, data: ^Data, recs: ^Record = nil) -> Result {
	n0 := expression_get_float(&fn.args[0], recs) or_return
	n1 := expression_get_float(&fn.args[1], recs) or_return
	data^ = n0 + n1
	return .Ok
}

sql_op_plus_s :: proc(fn: ^Expr_Function, data: ^Data, recs: ^Record = nil) -> Result {
	return not_implemented()
}

sql_op_minus_i :: proc(fn: ^Expr_Function, data: ^Data, recs: ^Record = nil) -> Result {
	n0 := expression_get_int(&fn.args[0], recs) or_return
	n1 := expression_get_int(&fn.args[1], recs) or_return
	data^ = overflow_safe_minus_i(n0, n1) or_return
	return .Ok
}

sql_op_minus_f :: proc(fn: ^Expr_Function, data: ^Data, recs: ^Record = nil) -> Result {
	n0 := expression_get_float(&fn.args[0], recs) or_return
	n1 := expression_get_float(&fn.args[1], recs) or_return
	data^ = n0 - n1
	return .Ok
}

sql_op_mult_i :: proc(fn: ^Expr_Function, data: ^Data, recs: ^Record = nil) -> Result {
	n0 := expression_get_int(&fn.args[0], recs) or_return
	n1 := expression_get_int(&fn.args[1], recs) or_return
	data^ = overflow_safe_mult_i(n0, n1) or_return
	return .Ok
}

sql_op_mult_f :: proc(fn: ^Expr_Function, data: ^Data, recs: ^Record = nil) -> Result {
	n0 := expression_get_float(&fn.args[0], recs) or_return
	n1 := expression_get_float(&fn.args[1], recs) or_return
	data^ = n0 * n1
	return .Ok
}

sql_op_divi_i :: proc(fn: ^Expr_Function, data: ^Data, recs: ^Record = nil) -> Result {
	n0 := expression_get_int(&fn.args[0], recs) or_return
	n1 := expression_get_int(&fn.args[1], recs) or_return
	data^ = n0 / n1
	return .Ok
}

sql_op_divi_f :: proc(fn: ^Expr_Function, data: ^Data, recs: ^Record = nil) -> Result {
	n0 := expression_get_float(&fn.args[0], recs) or_return
	n1 := expression_get_float(&fn.args[1], recs) or_return
	data^ = n0 / n1
	return .Ok
}

sql_op_mod_i :: proc(fn: ^Expr_Function, data: ^Data, recs: ^Record = nil) -> Result {
	n0 := expression_get_int(&fn.args[0], recs) or_return
	n1 := expression_get_int(&fn.args[1], recs) or_return
	data^ = n0 % n1
	return .Ok
}

sql_op_bit_or :: proc(fn: ^Expr_Function, data: ^Data, recs: ^Record = nil) -> Result {
	n0 := expression_get_int(&fn.args[0], recs) or_return
	n1 := expression_get_int(&fn.args[1], recs) or_return
	data^ = n0 | n1
	return .Ok
}

sql_op_bit_and :: proc(fn: ^Expr_Function, data: ^Data, recs: ^Record = nil) -> Result {
	n0 := expression_get_int(&fn.args[0], recs) or_return
	n1 := expression_get_int(&fn.args[1], recs) or_return
	data^ = n0 & n1
	return .Ok
}

sql_op_bit_xor :: proc(fn: ^Expr_Function, data: ^Data, recs: ^Record = nil) -> Result {
	n0 := expression_get_int(&fn.args[0], recs) or_return
	n1 := expression_get_int(&fn.args[1], recs) or_return
	data^ = n0 ~ n1
	return .Ok
}

sql_op_bit_not :: proc(fn: ^Expr_Function, data: ^Data, recs: ^Record = nil) -> Result {
	n := expression_get_int(&fn.args[0], recs) or_return
	data^ = ~n
	return .Ok
}

sql_op_unary_minus_i :: proc(fn: ^Expr_Function, data: ^Data, recs: ^Record = nil) -> Result {
	n := expression_get_int(&fn.args[0], recs) or_return
	if n == bits.I64_MIN {
		fmt.eprintf("Arithmetic overflow `-(%d)'\n", n)
		return .Error
	}
	data^ = -n
	return .Ok
}

sql_op_unary_minus_f :: proc(fn: ^Expr_Function, data: ^Data, recs: ^Record = nil) -> Result {
	n := expression_get_float(&fn.args[0], recs) or_return
	data^ = -n
	return .Ok
}

sql_op_unary_plus_i :: proc(fn: ^Expr_Function, data: ^Data, recs: ^Record = nil) -> Result {
	n := expression_get_int(&fn.args[0], recs) or_return
	data^ = n
	return .Ok
}

sql_op_unary_plus_f :: proc(fn: ^Expr_Function, data: ^Data, recs: ^Record = nil) -> Result {
	n := expression_get_float(&fn.args[0], recs) or_return
	data^ = n
	return .Ok
}

