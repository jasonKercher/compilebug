//+private
package streamql

import "fastrecs"

Record_Data :: union {
	fastrecs.Record,
}

Record :: struct {
	data: Record_Data,
	fields: []string,
	offset: i64,
	idx: i64,
	next: ^Record,
	ref: ^Record,
	select_len: i32,
	ref_count: i16,
	root_fifo_idx: u8,
	src_idx: u8,
}

destroy_record :: proc(rec: ^Record) {

}

record_get :: proc(rec: ^Record, src_idx: u8) -> ^Record {
	rec := rec
	for ; rec != nil; rec = rec.next {
		if rec.src_idx == src_idx {
			return rec
		}
	}
	return nil
}

record_get_line :: proc(rec: ^Record) -> string {
	switch v in rec.data {
	case fastrecs.Record:
		return fastrecs.get_line_from_record(v)
	}
	unreachable()
}
