package util

import "core:path/filepath"
import "core:fmt"

Fuzzy_Result :: enum {
	Found,
	Ambiguous,
	Not_Found,
}

@(private = "file")
_delete_file_list :: proc(files: []string) {
	files := files
	for f in &files {
		delete(f)
	}
	delete(files)
}

fuzzy_file_match :: proc(name: string) -> (string, Fuzzy_Result) {
	dir := filepath.dir(name)
	files := get_files_from_dir(dir)
	defer _delete_file_list(files)

	/* exact match */
	for f in files {
		if f == name {
			return fmt.aprintf("%s/%s", dir, f), nil
		}
	}

	matches := 0
	file_name: string

	/* exact match no case */
	for f in files {
		if string_compare_nocase(f, name) == 0 {
			matches += 1
			if matches > 1 {
				return file_name, .Ambiguous
			}
			file_name = fmt.aprintf("%s/%s", dir, f)
		}
	}

	if matches > 0 {
		return file_name, nil
	}

	/* no extension match */
	for f in files {
		f_no_ext := name_no_ext(f)
		if f_no_ext == name {
			matches += 1
			if matches > 1 {
				return file_name, .Ambiguous
			}
			file_name = fmt.aprintf("%s/%s", dir, f)
		}
	}

	if matches > 0 {
		return file_name, nil
	}

	/* no extension match no case */
	for f in files {
		f_no_ext := name_no_ext(f)
		if string_compare_nocase(f_no_ext, name) == 0 {
			matches += 1
			if matches > 1 {
				return file_name, .Ambiguous
			}
			file_name = fmt.aprintf("%s/%s", dir, f)
		}
	}

	return file_name, matches > 0 ? nil : .Not_Found
}

