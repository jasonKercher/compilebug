//+private
package streamql

import "bytemap"

Join_Side :: enum {
	Left,
	Right,
	Mixed,
}

Hash_Join :: struct {
	hash_data: bytemap.Multi(u64),
	left_expr: ^Expression,
	right_expr: ^Expression,
	held_records: [dynamic]Record,
	rec_idx: uint,
	state: Join_Side,
	comp_type: Data_Type,
}

new_hash_join :: proc() -> ^Hash_Join {
	hj := new(Hash_Join)
	hj.state = .Left
	return hj
}

hash_join_init :: proc(src: ^Source) {
	not_implemented()
}

hash_join_reset :: proc(join: ^Hash_Join) {
	if join == nil {
		return
	}
	not_implemented()
}
