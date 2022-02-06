//+private
package streamql

import "core:c"
import "core:io"
import "core:os"
import "core:fmt"
import "linkedlist"
import "core:bufio"
import "core:strings"
import "core:path/filepath"

foreign import libc "system:c"
foreign libc {
	@(link_name="mkstemp") _libc_mkstemp :: proc(template: cstring) -> c.int ---
}

Write_Record_Call :: proc(w: ^Writer, exprs: []Expression, recs: ^Record, bufw: ^bufio.Writer = nil) -> (int, Process_Result)

Writer_Data :: union {
	Delimited_Writer,
	Fixed_Writer,
	Subquery_Writer,
}

Writer :: struct {
	data: Writer_Data,
	write_record__: Write_Record_Call,
	writer: bufio.Writer,
	file_name: string,
	temp_name: string,
	temp_node: ^linkedlist.Node(string),
	fd: os.Handle,
	type: Io,
	is_detached: bool,
}

make_writer :: proc(sql: ^Streamql, write_io: Io) -> Writer {
	new_writer := Writer {
		type = write_io,
		fd = os.stdout,
	}
	switch write_io {
	case .Delimited:
		new_writer.write_record__ = _delimited_write_record
		new_writer.data = make_delimited_writer()
	case .Fixed:
		new_writer.data = make_fixed_writer()
	case .Subquery:
		new_writer.data = make_subquery_writer()
	}

	return new_writer
}

destroy_writer :: proc(w: ^Writer) {
	not_implemented()
}

writer_open :: proc(w: ^Writer, file_name: string) -> Result {
	_set_file_name(w, file_name)
	if _is_open(w) {
		fmt.eprintf("writer already open")
		return .Error
	}
	return _make_temp_file(w)
}

writer_close :: proc(w: ^Writer) -> Result {
	bufio.writer_flush(&w.writer)
	ios := bufio.writer_to_stream(&w.writer)
	io.destroy(ios)
	bufio.writer_destroy(&w.writer)

	if !_is_open(w) {
		return .Ok
	}

	os.close(w.fd)
	w.fd = -1

	if w.is_detached {
		w.is_detached = false
		return .Ok
	}

	if os.rename(w.temp_name, w.file_name) != os.ERROR_NONE {
		fmt.eprintf("rename failed\n")
		return .Error
	}

	/* chmod ?? */

	linkedlist.remove(&_global_remove_list, w.temp_node)
	w.temp_node = nil
	return .Ok
}

writer_resize :: proc(w: ^Writer, n: int) {
	//not_implemented()
}

writer_set_delim :: proc(w: ^Writer, delim: string) {
	#partial switch v in &w.data {
	case Delimited_Writer:
		v.delim = delim
	}
}

writer_set_rec_term :: proc(w: ^Writer, rec_term: string) {
	#partial switch v in &w.data {
	case Delimited_Writer:
		v.rec_term = rec_term
	case Fixed_Writer:
		v.rec_term = rec_term
	}
}

writer_take_file_name :: proc(w: ^Writer) -> string {
	w.is_detached = true
	file_name := w.file_name
	w.file_name = ""
	return file_name
}

writer_export_temp :: proc(w: ^Writer) -> (file_name: string, res: Result) {
	if !_is_open(w) {
		_make_temp_file(w) or_return
	}
	if w.is_detached {
		linkedlist.remove(&_global_remove_list, w.temp_node)
		return w.temp_name, .Ok
	} else {
		return w.file_name, .Ok
	}
}

@(private = "file")
_make_temp_file :: proc(w: ^Writer) -> Result {
	dir_name := "."
	if w.file_name != "" {
		dir_name = filepath.dir(w.file_name)
	}
	temp_name := fmt.tprintf("%s/_write_XXXXXX", dir_name)
	temp_name_cstr := strings.clone_to_cstring(temp_name, context.temp_allocator)

	w.fd = os.Handle(_libc_mkstemp(temp_name_cstr))
	if w.fd == -1 {
		fmt.eprintln("mkstemp fail")
		return .Error
	}

	w.temp_node = linkedlist.push(&_global_remove_list, temp_name)
	io_writer, ok := io.to_writer(os.stream_from_handle(w.fd))
	if !ok {
		fmt.eprintln("to_writer fail")
		return .Error
	}
	bufio.writer_init(&w.writer, io_writer)

	return .Ok
}

@(private = "file")
_set_file_name :: proc(w: ^Writer, file_name: string) {
	w.file_name = file_name
	w.is_detached = false
}

@(private = "file")
_is_open :: proc(w: ^Writer) -> bool {
	return w.fd != -1 && w.fd != os.stdout
}

/** Delimited_Writer **/

Delimited_Writer :: struct {
	delim: string,
	rec_term: string,
}

make_delimited_writer :: proc() -> Delimited_Writer {
	return Delimited_Writer {
		delim = ",",
		rec_term = "\n",
	}
}

_delimited_write_record :: proc(w: ^Writer, exprs: []Expression, recs: ^Record, bufw: ^bufio.Writer = nil) -> (int, Process_Result) {
	bufw := bufw
	exprs := exprs
	if bufw == nil {
		bufw = &w.writer
	}

	delimw := &w.data.(Delimited_Writer)
	written_len := 0
	n: int

	for expr, i in &exprs {
		if i > 0 {
			n, _ = bufio.writer_write_string(bufw, delimw.delim)
			written_len += n
		}

		if aster, is_aster := expr.data.(Expr_Asterisk); is_aster {
			src_idx := i32(aster)
			rec := record_get(recs, u8(src_idx))
			full_rec := record_get_line(rec)
			n, _ = bufio.writer_write_string(bufw, full_rec)
			written_len += n
			continue
		}

		/* TODO: type check on strict mode */

		s, res := expression_get_string(&expr, recs)
		if res == .Error {
			return written_len, .Error
		}
		n, _ = bufio.writer_write_string(bufw, s)
		written_len += n
	}

	n, _ = bufio.writer_write_string(bufw, delimw.rec_term)
	written_len += n

	return written_len, .Ok
}

/** Fixed_Writer **/

Fixed_Writer :: struct {
	rec_term: string,
}

make_fixed_writer :: proc() -> Fixed_Writer {
	return Fixed_Writer {}
}

/** Subquery_Writer **/

Subquery_Writer :: struct {

}

make_subquery_writer :: proc() -> Subquery_Writer {
	return Subquery_Writer {}
}

