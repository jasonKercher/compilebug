//+private
package streamql


Record :: struct {
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
	return ""
}
