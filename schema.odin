//+private
package streamql

import "core:path/filepath"
import "core:math/bits"
import "core:strings"
import "core:fmt"
import "core:os"
import "util"

Schema_Props :: enum {
	Is_Var,
	Is_Const,
	Is_Default,
	Is_Preresolved,
	Delim_Set,
	Must_Run_Once,
}

Schema_Item :: struct {
	name: string,
	loc: i32,
	width: i32,
}

Schema_Data :: union {
	Reader,
	Writer,
}

Schema :: struct {
	data: Schema_Data,
	layout: [dynamic]Schema_Item,
	name: string,
	schema_path: string,
	delim: string,
	rec_term: string,
	props: bit_set[Schema_Props],
}
