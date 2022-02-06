//+private
package streamql

Data_Type :: enum {
	Int,
	Float,
	String,
}

Data :: union {
	i64,
	f64,
	string,
}

data_determine_type :: proc(t0, t1: Data_Type) -> Data_Type {
	if t0 == t1 {
		return t0
	}
	if t0 == .String {
		return t1
	}
	if t1 == .String {
		return t0
	}
	return .Float
}

data_to_int :: proc(data: ^Data, type: Data_Type) -> (i64, Result) {
	switch type {
	case .Int:
		return data.(i64), .Ok
	case .Float:
		return i64(data.(f64)), .Ok
	case .String:
		return 0, not_implemented()
	}
	unreachable()
}

data_to_float :: proc(data: ^Data, type: Data_Type) -> (f64, Result) {
	switch type {
	case .Int:
		return f64(data.(i64)), .Ok
	case .Float:
		return data.(f64), .Ok
	case .String:
		return 0, not_implemented()
	}
	unreachable()
}

data_to_string :: proc(data: ^Data, type: Data_Type) -> (string, Result) {
	switch type {
	case .Int:
		return "", not_implemented()
	case .Float:
		return "", not_implemented()
	case .String:
		return data.(string), .Ok
	}
	unreachable()
}
