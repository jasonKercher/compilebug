//+private
package streamql

Scope_Status :: enum {
	Is_In_Block,
}

Scope :: struct {
	parent: ^Scope,
	status: bit_set[Scope_Status],
	var_map: map[string]i32,
}

make_scope :: proc() -> Scope {
	return Scope {
		var_map = make(map[string]i32),
	}
}
