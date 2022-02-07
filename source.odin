//+private
package streamql

import "core:strings"

Source_Props :: enum {
	Must_Reopen,
	Is_Stdin,
}

Join_Type :: enum {
	From,
	Inner,
	Left,
	Right,
	Full,
	Cross,
}

Source_Data :: union {
	^Query,
	string,
}

Source :: struct {
	data: Source_Data,
	alias: string,
	schema: Schema,
	props: bit_set[Source_Props],
}

construct_source_name :: proc(src: ^Source, name: string) {
	src^ = {
		data = strings.clone(name),
	}
}

construct_source_subquery :: proc(src: ^Source, subquery: ^Query) {
	src^ = {
		data = subquery,
	}
}

construct_source :: proc {
	construct_source_name,
	construct_source_subquery,
}

source_reset :: proc(src: ^Source, has_executed: bool) -> Result {
	if .Must_Reopen in src.props {
		reader := &src.schema.data.(Reader)
		reader_reopen(reader) or_return
		reader.reset__(reader) or_return
	} else if has_executed {
		reader := &src.schema.data.(Reader)
		//reader_reopen(reader) or_return
		reader.reset__(reader) or_return
	}
	return .Ok
}

source_resolve_schema :: proc(sql: ^Streamql, src: ^Source) -> Result {
	if .Is_Preresolved in src.schema.props {
		return .Ok
	}

	r := &src.schema.data.(Reader)

	delim: string

	return .Ok
}
