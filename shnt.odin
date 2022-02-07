package streamql

import "fifo"

main :: proc()
{
	sql: Streamql
	_build(&sql, sql.queries[0])
}

Plan :: struct {
	execute_vector: []Process,
	root_fifos: []fifo.Fifo(^Record),
	_root_data: []Record,
}

_activate_procs :: proc(sql: ^Streamql, q: ^Query) {
	union_pipes := 0
	graph_size := 0
	proc_count := graph_size + union_pipes
	fifo_base_size := 0

	root_fifo_vec := make([dynamic]fifo.Fifo(^Record))

	pipe_count := 0

	process: Process
	
	//for node in &q.plan.proc_graph.nodes {
		process_activate(&process, &root_fifo_vec, &pipe_count, fifo_base_size)
	//}

	if len(root_fifo_vec) == 0 {
		return
	}

	root_size := fifo_base_size * pipe_count
	for f in &root_fifo_vec {
		fifo.set_size(&f, u16(root_size / len(root_fifo_vec) + 1))
	}
	
	q.plan._root_data = make([]Record, root_size)
	q.plan.root_fifos = root_fifo_vec[:]
}

_build :: proc(sql: ^Streamql, q: ^Query, is_union: bool = false) -> Result {
	for subq in &q.subquery_exprs {
		_build(sql, subq) or_return
	}

	_activate_procs(sql, q)

	return .Ok
}

Process_State :: enum u8 {
	Is_Const,
	Is_Dual_Link,
	Is_Enabled,
	Is_Passive,
	Is_Op_True,
	In0_Always_Dead,
	Kill_In0,
	Kill_In1,
	Wait_In0,
	Wait_In0_End,
	Root_Fifo0,
	Root_Fifo1,
	Needs_Aux,
	Is_Secondary,
	Has_Second_Input,
}

Process_Data :: union {
	^Select,
}

Process_Call :: proc(process: ^Process) -> Result

Process_Unions :: struct #raw_union {
	p: []^Process,
}

Process :: struct {
	data: Process_Data,
	action__: Process_Call,
	root_fifo_ref: ^[]fifo.Fifo(^Record),
	wait_list: []^Process,
	input: [2]^fifo.Fifo(^Record),
	output: [2]^fifo.Fifo(^Record),
	aux_root: ^fifo.Fifo(^Record),
	union_data: Process_Unions,
	msg: string,
	rows_affected: int,
	_in_buf: []^Record,
	state: bit_set[Process_State],
}

process_activate :: proc(process: ^Process, root_fifo_vec: ^[dynamic]fifo.Fifo(^Record), pipe_count: ^int, base_size: int) {
	_new_fifo_root :: proc(root_fifo_vec: ^[dynamic]fifo.Fifo(^Record), pipe_count: ^int) -> ^fifo.Fifo(^Record) {
		append(root_fifo_vec, fifo.make_fifo(^Record))
		pipe_count^ += 1
		return &root_fifo_vec[len(root_fifo_vec) - 1]
	}

	process._in_buf = make([]^Record, u16(base_size))
	if .Root_Fifo0 in process.state {
		process.input[0] = _new_fifo_root(root_fifo_vec, pipe_count)
	} else if .Root_Fifo1 in process.state {
		process.input[1] = _new_fifo_root(root_fifo_vec, pipe_count)
	}

	if select, is_select := process.data.(^Select); is_select {
	}

	//for node in process.union_data.n {
	//	pipe_count^ += 1
	//	node.data.output[0] = fifo.new_fifo(^Record, u16(base_size))
	//	node.data.output[0].input_count = 1
	//}

	if process.input[0] == nil {
		pipe_count^ += 1
		process.input[0] = fifo.new_fifo(^Record, u16(base_size))
		/* NOTE: GROUP BY hack. a constant query expression
		 *       containing a group by essentially has 2 roots.
		 *       We just give in[0] a nudge (like a root).
		 */
		if .Is_Const in process.state {
			fifo.advance(process.input[0])
		}
	}

	if .Has_Second_Input in process.state {
		pipe_count^ += 1
		process.input[1] = fifo.new_fifo(^Record, u16(base_size))
	}

	if .Kill_In0 in process.state {
		process.input[0].is_open = false
	}

	if .Kill_In1 in process.state {
		process.input[1].is_open = false
	}
	
	if .Needs_Aux in process.state {
		pipe_count^ += 1
		process.aux_root = fifo.new_fifo(^Record, u16(base_size))
	}
}

Query :: struct {
	operation: Select,
	plan: Plan,
	unions: [dynamic]^Query,
	subquery_exprs: [dynamic]^Query,
	var_source_vars: [dynamic]i32,
	var_sources: [dynamic]i32,
	var_expr_vars: [dynamic]i32,
}



Record :: struct {
	fields: []string,
	next: ^Record,
	ref: ^Record,
}


Select :: struct {
	select_list: [dynamic]^Select,
	top_count: i64,
	offset: i64,
	row_num: i64,
	rows_affected: i64,
	select_idx: i32,
}

sqlprocess_recycle :: proc(_p: ^Process, recs: ^Record) {
	recs := recs
	for recs != nil {
		root_fifo := &_p.root_fifo_ref[0]
		if recs.ref != nil {
				sqlprocess_recycle(_p, recs.ref)
			recs.ref = nil
		}

		next_rec := recs.next
		recs.next = nil

		recs = next_rec
	}
}
Result :: enum u8 {
	Ok,
}

Streamql :: struct {
	queries: [dynamic]^Query,
}

