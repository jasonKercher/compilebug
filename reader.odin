//+private
package streamql

import "core:os"
import "fastrecs"

/* maybe come up with a better solution?? */
Delete_Call :: proc(r: ^Reader)
Reset_Call :: proc(r: ^Reader) -> Result
Get_Record_Call :: proc(r: ^Reader, rec: ^Record) -> Result
Get_Record_At_Call :: proc(r: ^Reader, rec: ^Record, offset: i64) -> Result

Reader_Status :: enum u8 {
	Eof,
}

Reader_Data :: union {
	fastrecs.Reader,
}

Reader :: struct {
	delete__: Delete_Call,
	reset__: Reset_Call,
	get_record__: Get_Record_Call,
	get_record_at__: Get_Record_At_Call,
	file_name: string,
	data: Reader_Data,
	first_rec: Record, // Put this somewhere else...
	record_idx: i64,
	random_access_file: os.Handle,
	max_field_idx: i32,
	skip_rows: i32,
	type: Io,
	status: bit_set[Reader_Status],
}

make_reader :: proc() -> Reader {
	return Reader {
		random_access_file = -1,
	}
}

reader_reopen :: proc(reader: ^Reader) -> Result {
	return not_implemented()
}

reader_assign :: proc(sql: ^Streamql, src: ^Source) -> Result {
	reader := &src.schema.data.(Reader)
	switch reader.type {
	case .Delimited:
		r := fastrecs.make_reader()
		#partial switch sql.in_quotes {
		case .Rfc4180:
			r.quote_style = .Rfc4180
		case .Weak:
			r.quote_style = .Weak
		case .None:
			r.quote_style = .None
		}
		if sql.in_delim != "" {
			fastrecs.set_delim(&r, sql.in_delim)
		}
		reader.data = r
		reader.delete__ = delimited_delete
		reader.get_record__ = delimited_get_record
		reader.get_record_at__ = delimited_get_record_at
		if .Is_Stdin in src.props {
			reader.reset__ = delimited_reset_stdin
			return .Ok
		}
		reader.reset__ = delimited_reset
		if fastrecs.open(&reader.data.(fastrecs.Reader), reader.file_name) == .Error {
			return .Error
		}
		return .Ok
	case .Fixed:
		return not_implemented()
	case .Subquery:
		return not_implemented()
	}
	unreachable()
}

reader_start_file_backed_input :: proc(r: ^Reader) {
	not_implemented()
}
