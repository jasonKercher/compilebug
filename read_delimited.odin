//+private
package streamql


delimited_delete :: proc(r: ^Reader) {
}

delimited_reset :: proc(r: ^Reader) -> Result {
	r.record_idx = 0
	r.status -= {.Eof}
	return .Ok
}

delimited_reset_stdin :: proc(r: ^Reader) -> Result {
	return not_implemented()
}

delimited_get_record :: proc(r: ^Reader, rec: ^Record) -> Result {
	rec := rec

	if r.random_access_file != -1 {
		return not_implemented()
	}

	rec.idx = r.record_idx
	r.record_idx += 1
	return .Ok
}

delimited_get_record_at :: proc(r: ^Reader, rec: ^Record, offset: i64) -> Result {
	r.status -= {.Eof}
	return delimited_get_record(r, rec)
}
