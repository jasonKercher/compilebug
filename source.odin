//+private
package streamql

import "core:strings"
import "fastrecs"

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
	joinable_logic: []^Logic,
	join_data: ^Hash_Join,
	join_logic: ^Logic_Group,
	join_type: Join_Type,
	props: bit_set[Source_Props],
}

construct_source_name :: proc(src: ^Source, name: string) {
	src^ = {
		data = strings.clone(name),
		schema = make_schema(),
	}
}

construct_source_subquery :: proc(src: ^Source, subquery: ^Query) {
	src^ = {
		data = subquery,
		schema = make_schema(),
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
		hash_join_reset(src.join_data)
	} else if has_executed {
		reader := &src.schema.data.(Reader)
		//reader_reopen(reader) or_return
		reader.reset__(reader) or_return
		hash_join_reset(src.join_data)
	}
	return .Ok
}

source_resolve_schema :: proc(sql: ^Streamql, src: ^Source) -> Result {
	if .Is_Preresolved in src.schema.props {
		return .Ok
	}

	r := &src.schema.data.(Reader)

	delim: string
	#partial switch r.type {
	case .Delimited:
		delim = r.data.(fastrecs.Reader)._delim
		src.schema.io = .Delimited
	case .Subquery:
		subquery := src.data.(^Query)
		sub_schema := op_get_schema(&subquery.operation)
		if sub_schema == &src.schema {
			return .Ok
		}
		delim = sub_schema.delim
		src.schema.write_io = sub_schema.write_io
	case .Fixed:
		src.schema.write_io = .Fixed
	case:
		return .Error
	}

	schema_set_delim(&src.schema, delim)
	return .Ok
}
