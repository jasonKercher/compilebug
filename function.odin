//+private
package streamql

import "core:reflect"
import "core:fmt"

OPERATOR_COUNT :: 11
FIELD_TYPE_COUNT :: 3

Function_Type :: enum {
	/* Standard Math */
	Plus,
	Minus,
	Multiply,
	Divide,
	Modulus,
	Bit_Or,
	Bit_And,
	Bit_Xor,

	/* Unary */
	Plus_Unary,
	Minus_Unary,
	Bit_Not_Unary,

	/* Functions */
	Abs,
	Ascii,
	Cast,
	Ceiling,
	Char,
	Charindex,
	Coalesce,
	Concat,
	Convert,
	Datalength,
	Dateadd,
	Datediff,
	Datename,
	Datepart,
	Floor,
	Getdate,
	Getutcdate,
	Isdate,
	Isnull,
	Isnumeric,
	Left,
	Len,
	Lower,
	Ltrim,
	Nchar,
	Nullif,
	Patindex,
	Rand,
	Replace,
	Right,
	Round,
	Rtrim,
	Space,
	Stuff,
	Substring,
	Try_Cast,
	Upper,
	User_Name,
}

_scalar_ops : [OPERATOR_COUNT][FIELD_TYPE_COUNT] Function_Call = {
        {sql_op_plus_i,        sql_op_plus_f,        sql_op_plus_s},
        {sql_op_minus_i,       sql_op_minus_f,       nil},
        {sql_op_mult_i,        sql_op_mult_f,        nil},
        {sql_op_divi_i,        sql_op_divi_f,        nil},
        {sql_op_mod_i,         nil,                  nil},
        {sql_op_bit_or,        nil,                  nil},
        {sql_op_bit_and,       nil,                  nil},
        {sql_op_bit_xor,       nil,                  nil},
        {sql_op_bit_not,       nil,                  nil},
        {sql_op_unary_minus_i, sql_op_unary_minus_f, nil},
        {sql_op_unary_plus_i,  sql_op_unary_plus_f,  nil},
}

Function_Call :: proc(fn: ^Expr_Function, data: ^Data, recs: ^Record) -> Result

Expr_Function :: struct {
	call__: Function_Call,
	args: [dynamic]Expression,
	min_args: u16,
	max_args: u16,
	type: Function_Type,
	data_type: Data_Type,
}

make_function :: proc(fn_type: Function_Type) -> Expr_Function {
	return Expr_Function {
		type = fn_type,
		data_type = .String,
	}
}

destroy_function :: proc(fn: ^Expr_Function) {
	if fn == nil {
		return
	}
	delete(fn.args)
}

function_add_expression :: proc(fn: ^Expr_Function, expr: ^Expression) -> ^Expression {
	if cap(fn.args) == 0 {
		fn.args = make([dynamic]Expression)
	}
	append(&fn.args, expr^)
	return &fn.args[len(fn.args) - 1]
}

function_op_resolve :: proc(fn_expr: ^Expression, data: Expression_Data) -> Result {
	fn := &fn_expr.data.(Expr_Function)
	if int(fn.type) > int(Function_Type.Abs) {
		return .Ok
	}

	fn.min_args = 2
	fn.max_args = 2

	expr0 := &fn.args[0]
	expr1 := expr0

	if len(fn.args) > 1 {
		expr1 = &fn.args[1]
	}

	fn_expr.data_type = data_determine_type(expr0.data_type, expr1.data_type)

	#partial switch fn.type {
	case .Plus_Unary:
		fallthrough
	case .Minus_Unary:
		fallthrough
	case .Bit_Not_Unary:
		fn.min_args = 1
		fn.max_args = 1
	}

	fn.call__ = _scalar_ops[fn.type][fn_expr.data_type]
	if fn.call__ == nil {
		names := reflect.enum_field_names(typeid_of(Function_Type))
		fmt.eprintf("invalid type for `%s' operation\n", names[fn.type])
		return .Error
	}

	return .Ok
}

function_validate :: proc(fn: ^Expr_Function, expr: ^Expression) -> Result {
	if fn.type == .Isnull {
		/* WHY IS THIS HERE??? */
		return not_implemented()
	}

	arg_count := u16(len(fn.args))
	if arg_count >= fn.min_args && arg_count <= fn.max_args {
		return .Ok
	}

	names := reflect.enum_field_names(typeid_of(Function_Type))
	fn_name := names[fn.type]
	if fn.min_args == fn.max_args {
		fmt.eprintf("function `%s' expected %d argument(s); found %d\n", fn_name, fn.min_args, arg_count)
	} else {
		fmt.eprintf("function `%s' expected %d - %d arguments; found %d\n", fn_name, fn.min_args, fn.max_args, arg_count)
	}
	return .Error
}
