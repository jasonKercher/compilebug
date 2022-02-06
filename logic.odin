//+private
package streamql

import "bytemap"

Logic_Group_Type :: enum {
	And,
	Or,
	Not,
	Predicate,
	Predicate_Negated,
}

Logic_Group :: struct {
	items: [2]^Logic_Group,
	joinable: [dynamic]^Logic_Group,
	join_logic: ^Logic,
	condition: ^Logic,
	type: Logic_Group_Type,
}

new_logic_group :: proc(type: Logic_Group_Type) -> ^Logic_Group {
	lg := new(Logic_Group)
	return lg
}

free_logic_group :: proc(lg: ^Logic_Group) {
	free(lg)
}

logic_group_get_condition_count :: proc(lg: ^Logic_Group) -> int {
	not_implemented()
	return -1
}

Comparison :: enum {
	None = -3,
	False = -2,
	True = -1,
	Eq = 0,
	Ne = 1,
	Gt = 2,
	Ge = 3,
	Lt = 4,
	Le = 5,
	Like = 6,
	Not_Like = 7,
	Null = 8,
	In = 9,
	Not_In = 10,
	Sub_In = 11,
	Sub_Not_In = 12,
}

Logic :: struct {
	exprs: [2]Expression,
	comp_type: Comparison,
	data_type: Data_Type,
}

new_logic :: proc() -> ^Logic {
	l := new(Logic)
	l^ = {
		comp_type = .None,
	}
	return l
}

logic_add_expression :: proc(l: ^Logic, expr: ^Expression) -> ^Expression {
	if l.exprs[0].data == nil {
		l.exprs[0] = expr^
		return &l.exprs[0]
	}
	l.exprs[1] = expr^
	return &l.exprs[1]
}

logic_assign_process :: proc(l: ^Logic, logic_proc: ^Process) -> Result {
	return not_implemented()
}

logic_must_be_true :: proc(lg: ^Logic_Group, l: ^Logic) -> bool {
	not_implemented()
	return false
}

In_Data :: union {
	^Query,
	[dynamic]Expression,
}

In_List :: struct {
	data: In_Data,
	list: bytemap.Set,
	return_state: int,
}
