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

Writer_Data :: union {
	Delimited_Writer,
	Fixed_Writer,
	Subquery_Writer,
}

Writer :: struct {
	data: Writer_Data,
	writer: bufio.Writer,
	file_name: string,
	temp_name: string,
	temp_node: ^linkedlist.Node(string),
	fd: os.Handle,
	is_detached: bool,
}

make_writer :: proc(sql: ^Streamql) -> Writer {
	new_writer := Writer {
		fd = os.stdout,
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

