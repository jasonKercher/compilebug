package streamql

import "bigraph"
import "fifo"

main :: proc()
{
	sql: Streamql
	plan_build(&sql)
}

Plan_State :: enum {
	Has_Stepped,
	Is_Complete,
	Is_Const,
}

Plan :: struct {
	execute_vector: []Process,
	root_fifos: []fifo.Fifo(^Record),
	proc_graph: bigraph.Graph(Process),
	op_true: ^bigraph.Node(Process),
	op_false: ^bigraph.Node(Process),
	curr: ^bigraph.Node(Process),
	_root_data: []Record,
	plan_str: string,
	rows_affected: u64,
	state: bit_set[Plan_State],
	src_count: u8,
	id: u8,
}

plan_build :: proc(sql: ^Streamql) -> Result {
	for q in &sql.queries {
		_build(sql, q) or_return
	}
	return .Ok
}

_activate_procs :: proc(sql: ^Streamql, q: ^Query) {
	graph_size := len(q.plan.proc_graph.nodes)
	union_pipes := 0
	proc_count := graph_size + union_pipes
	fifo_base_size := 0

	root_fifo_vec := make([dynamic]fifo.Fifo(^Record))

	pipe_count := 0
	
	for node in &q.plan.proc_graph.nodes {
		process_activate(&node.data, &root_fifo_vec, &pipe_count, fifo_base_size)
	}

	if len(root_fifo_vec) == 0 {
		return
	}

	root_size := fifo_base_size * pipe_count
	for f in &root_fifo_vec {
		fifo.set_size(&f, u16(root_size / len(root_fifo_vec) + 1))
	}
	
	q.plan._root_data = make([]Record, root_size)
	q.plan.root_fifos = root_fifo_vec[:]


	for node in &q.plan.proc_graph.nodes {
		node.data.root_fifo_ref = &q.plan.root_fifos
	}

}

_build :: proc(sql: ^Streamql, q: ^Query, entry: ^bigraph.Node(Process) = nil, is_union: bool = false) -> Result {
	for subq in &q.subquery_exprs {
		_build(sql, subq) or_return
	}


	//_print(&q.plan)

	bigraph.set_roots(&q.plan.proc_graph)
	bigraph.set_roots(&q.plan.proc_graph)


	if len(q.plan.proc_graph.nodes) == 0 {
		if entry != nil {
			entry.data.state += {.Is_Const}
		}
		return .Ok
	}

	for subq in &q.subquery_exprs {
		bigraph.consume(&q.plan.proc_graph, &subq.plan.proc_graph)
	}

	bigraph.set_roots(&q.plan.proc_graph)


	/* Only non-subqueries beyond this point */
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

Process_Call :: proc(process: ^Process) -> Process_Result

Process_Unions :: struct #raw_union {
	n: []^bigraph.Node(Process),
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
		if .Is_Const in process.state {
			select.schema.props += {.Is_Const}
		}
	}

	for node in process.union_data.n {
		pipe_count^ += 1
		node.data.output[0] = fifo.new_fifo(^Record, u16(base_size))
		node.data.output[0].input_count = 1
	}

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

Schema_Props :: enum {
	Is_Var,
	Is_Const,
	Is_Default,
	Is_Preresolved,
	Delim_Set,
	Must_Run_Once,
}

Schema :: struct {
	props: bit_set[Schema_Props],
}


Select_Call :: proc(sel: ^Select, recs: ^Record) -> Process_Result

Select :: struct {
	select__: Select_Call,
	schema: Schema,
	select_list: [dynamic]^Select,
	top_count: i64,
	offset: i64,
	row_num: i64,
	rows_affected: i64,
	select_idx: i32,
}

_select :: proc(sel: ^Select, recs: ^Record) -> Process_Result {
	sel.row_num += 1


	if recs == nil {
		return .Ok
	}

	return .Ok
}

Process_Result :: enum {
	Ok,
	Error,
	Complete,
	Running,
	Waiting_In0,
	Waiting_In1,
	Waiting_In_Either,
	Waiting_In_Both,
	Waiting_Out0,
	Waiting_Out1,
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

sql_select :: proc(_p: ^Process) -> Process_Result {
	main_select := _p.data.(^Select)
	current_select := main_select.select_list[main_select.select_idx]

	in_ := _p.input[0]
	out := _p.output[0]

	if out != nil && !out.is_open {
		for union_proc in _p.union_data.p {
			fifo.set_open(union_proc.output[0], false)
		}
		return .Complete
	}

	if .Wait_In0 not_in _p.state {
		/* subquery reads expect union schema to
		 * be "in sync" with the subquery select's
		 * current schema
		 */
		if out != nil && !fifo.is_empty(out) {
			return .Running
		}


		return .Complete
	}

	if fifo.is_empty(in_) {
		if .Wait_In0 in _p.state && in_.is_open {
			return .Waiting_In0
		}
		_p.state -= {.Wait_In0}
	}

	if out != nil && fifo.receivable(out) == 0 {
		return .Waiting_Out0
	}

	////

	res := Process_Result.Waiting_In0

	/* TODO: DELETE ME */

	iters: u16 = 0
	for recs := fifo.begin(in_); recs != fifo.end(in_); {
		iters += 1 
		if main_select.rows_affected >= current_select.top_count {
			res = .Running
			break
		}

		main_select.select__(main_select, recs) or_return

		_p.rows_affected += 1
		main_select.rows_affected += 1

		if out != nil {
			fifo.add(out, recs)
			sqlprocess_recycle(_p, recs)
		}

		recs = fifo.iter(in_)

		if out != nil && fifo.receivable(out) == 0 {
			res = .Waiting_Out0
			break
		}
	}
	fifo.update(in_)

	if main_select.rows_affected >= current_select.top_count {
		_p.state -= {.Wait_In0}
		res = .Running
	}
	return res
}

Result :: enum u8 {
	Ok,
	Running,
	Error,
	Eof,
	Null, // refering to NULL in SQL
}

Streamql :: struct {
	schema_paths: [dynamic]string,
	queries: [dynamic]^Query,
}

