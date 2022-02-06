package util

import "core:os"
import "core:io"
import "core:fmt"
import "core:bufio"
import "core:bytes"
import "core:strings"
import "core:path/filepath"

string_compare_nocase :: proc(s0, s1: string) -> int {
	res: int
	min_len := min(len(s0), len(s1))
	for i := 0; res == 0 && i < min_len; i += 1 {
		res = int(to_lower_ascii(s0[i])) - int(to_lower_ascii(s1[i]))
	}
	return res == 0 ? len(s0) - len(s1) : res
}

string_compare_nocase_rtrim :: proc(s0, s1: string) -> int {
	res: int
	short := s0
	long := s1
	if len(s0) > len(s1) {
		short = s1
		long = s0
	}

	i := 0
	for ; res == 0 && i < len(short); i += 1 {
		res = int(to_lower_ascii(s0[i])) - int(to_lower_ascii(s1[i]))
	}

	for ; res == 0 && i < len(long); i += 1 {
		res = bytes.is_ascii_space(rune(long[i])) ? 0 : len(s0) - len(s1)
	}
	return res
}

stdin_to_string :: proc() -> string
{
	b : strings.Builder
	strings.init_builder(&b)
	stdin_reader, ok := io.to_reader(os.stream_from_handle(os.stdin))
	if !ok {
		fmt.eprintln("failed to build reader from stdin");
		os.exit(1)
	}

	buffered_input : bufio.Reader
	bufio.reader_init(&buffered_input, stdin_reader)

	c,err := bufio.reader_read_byte(&buffered_input)
	for ; err == .None ; c,err = bufio.reader_read_byte(&buffered_input) {
		/* Hmmm... Not breaking on c-d */
		//if c == 0x04 {
		//	break
		//}
		append(&b.buf, c)
	}

	bufio.reader_destroy(&buffered_input)
	io.destroy(stdin_reader)

	return strings.to_string(b)
}


foreign import libc "system:c"

foreign libc {
	@(link_name="readdir") _libc_readdir :: proc(dirp: os.Dir) -> ^os.Dirent ---
	@(link_name="opendir") _libc_opendir :: proc(name: cstring) -> os.Dir ---
}

get_files_from_dir :: proc(path: string) -> []string {
	cpath := strings.clone_to_cstring(path, context.temp_allocator)
	dirp := _libc_opendir(cpath)
	if dirp == nil {
		return nil
	}

	files: [dynamic]string

	entry := _libc_readdir(dirp)
	for ; entry != nil; entry = _libc_readdir(dirp) {
		append(&files, strings.clone_from_cstring(cstring(&entry.name[0])))
	}
	os._unix_closedir(dirp)

	return files[:]
}

to_lower_ascii :: proc(b: u8) -> u8 {
	if b >= 'A' && b <= 'Z' {
		return 'a' + (b - 'A')
	}
	return b
}

/* literally copied from slashpath */
name_no_ext :: proc(path: string) -> (name: string) {
	_, file := filepath.split(path)
	name = file
	for i := len(file)-1; i >= 0 && !filepath.is_separator(file[i]); i -= 1 {
		if file[i] == '.' {
			name = file[:i]
			return
		}
	}
	return file
}
