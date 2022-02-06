//+private
package streamql

import "fastrecs"

delimited_delete :: proc(r: ^Reader) {
	fastrecs.destroy(&r.data.(fastrecs.Reader))
}

delimited_reset :: proc(r: ^Reader) -> Result {
	fr_reader := &r.data.(fastrecs.Reader)
	r.record_idx = 0
	r.status -= {.Eof}
	fastrecs.reset(fr_reader)

	fr_rec: fastrecs.Record
	for i := 0; i < int(r.skip_rows); i += 1 {
		switch fastrecs.get_record(fr_reader, &fr_rec, int(r.max_field_idx) + 1) {
		case .Good:
		case .Reset:
			fallthrough
		case .Error:
			return .Error
		case .Eof:
			r.status += {.Eof}
			return .Eof
		}
	}
	return .Ok
}

delimited_reset_stdin :: proc(r: ^Reader) -> Result {
	return not_implemented()
}

delimited_get_record :: proc(r: ^Reader, rec: ^Record) -> Result {
	rec := rec
	fr_reader := &r.data.(fastrecs.Reader)
	fr_record := &rec.data.(fastrecs.Record)

	if .Eof in r.status {
		return .Eof
	}

	if r.random_access_file == -1 {
		rec.offset = fr_reader.offset
	}

	switch fastrecs.get_record(fr_reader, fr_record, int(r.max_field_idx) + 1) {
	case .Good:
	case .Reset:
		fallthrough
	case .Error:
		return .Error
	case .Eof:
		r.status += {.Eof}
		return .Eof
	}

	rec.fields = fr_record.fields

	if r.random_access_file != -1 {
		return not_implemented()
	}

	rec.idx = r.record_idx
	r.record_idx += 1
	return .Ok
}

delimited_get_record_at :: proc(r: ^Reader, rec: ^Record, offset: i64) -> Result {
	fr_reader := &r.data.(fastrecs.Reader)
	if fastrecs.seek(fr_reader, offset) == .Error {
		return .Error
	}
	r.status -= {.Eof}
	return delimited_get_record(r, rec)
}
