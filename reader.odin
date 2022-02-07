//+private
package streamql

import "core:os"

/* maybe come up with a better solution?? */
Delete_Call :: proc(r: ^Reader)
Reset_Call :: proc(r: ^Reader) -> Result
Get_Record_Call :: proc(r: ^Reader, rec: ^Record) -> Result
Get_Record_At_Call :: proc(r: ^Reader, rec: ^Record, offset: i64) -> Result

Reader_Status :: enum u8 {
	Eof,
}

Reader :: struct {
	delete__: Delete_Call,
	reset__: Reset_Call,
	get_record__: Get_Record_Call,
	get_record_at__: Get_Record_At_Call,
	file_name: string,
	first_rec: Record, // Put this somewhere else...
	record_idx: i64,
	random_access_file: os.Handle,
	max_field_idx: i32,
	skip_rows: i32,
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
	return .Ok
}

reader_start_file_backed_input :: proc(r: ^Reader) {
	not_implemented()
}
