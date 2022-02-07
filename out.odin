package streamql


import "core:math/bits"
import "core:strings"
import "core:bufio"
import "core:fmt"
import "core:io"
import "core:os"
import "bigraph"
import "fifo"


main :: proc()
{
	query_str : string

	cfg: bit_set[Config] = {}
	sql: Streamql
	construct(&sql, cfg)

		query_str = "select 1"

	if exec(&sql, query_str) == .Error {
		os.exit(2)
	}

	destroy(&sql)
}

//+private
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

make_plan :: proc() -> Plan {
	p := Plan {
		proc_graph = bigraph.make_graph(Process),
		op_true = bigraph.new_node(make_process(nil, "OP_TRUE")),
		op_false = bigraph.new_node(make_process(nil, "OP_FALSE")),
		curr = bigraph.new_node(make_process(nil, "START")),
	}
	p.curr.data.state += {.Is_Passive}

	bigraph.add_node(&p.proc_graph, p.curr)
	bigraph.add_node(&p.proc_graph, p.op_true)
	bigraph.add_node(&p.proc_graph, p.op_false)
	return p
}

destroy_plan :: proc(p: ^Plan) {
	bigraph.destroy(&p.proc_graph)
}

plan_reset :: proc(p: ^Plan) -> Result {
	if .Is_Complete not_in p.state {
		return .Ok
	}

	p.state -= {.Is_Complete}
	p.rows_affected = 0

	for node in &p.proc_graph.nodes {
		process_enable(&node.data)
	}

	_preempt(p)
	return .Ok
}

plan_print :: proc(sql: ^Streamql) {
	for q, i in &sql.queries {
		fmt.eprintf("\nQUERY %d\n", i + 1)
		_print(&q.plan)
	}
}

plan_build :: proc(sql: ^Streamql) -> Result {
	for q in &sql.queries {
		_build(sql, q) or_return
	}
	return .Ok
}

@(private = "file")
_preempt :: proc(p: ^Plan) {
	if len(p._root_data) == 0 {
		return
	}

	buf_idx := -1
	for rec, i in &p._root_data {
		rec.next = nil
		rec.root_fifo_idx = u8(i % len(p.root_fifos))
		if rec.root_fifo_idx == 0 {
			buf_idx += 1
		}

		p.root_fifos[rec.root_fifo_idx].buf[buf_idx] = &rec
	}

	for f in &p.root_fifos {
		fifo.set_full(&f)
	}
}

@(private = "file")
_from :: proc(sql: ^Streamql, q: ^Query) -> Result {
	return .Ok
}

@(private = "file")
_where :: proc(sql: ^Streamql, q: ^Query) -> Result {
	return .Ok
}

@(private = "file")
_group :: proc(sql: ^Streamql, q: ^Query) -> Result {
	return not_implemented()
}

@(private = "file")
_having :: proc(sql: ^Streamql, q: ^Query) -> Result {
	return .Ok
}

@(private = "file")
_operation :: proc(sql: ^Streamql, q: ^Query, entry: ^bigraph.Node(Process), is_union: bool = false) -> Result {
	prev := q.plan.curr
	prev.out[0] = q.plan.op_true
	q.plan.curr = q.plan.op_true

	/* Current no longer matters. After operation, we
	 * do order where current DOES matter... BUT
	 * if we are in a union we should not encounter
	 * ORDER BY...
	 */
	if is_union {
		q.plan.op_false.data.state += {.Is_Passive}
		if .Is_Passive in prev.data.state {
			q.plan.curr = prev
			return .Ok
		}
	}

	if entry != nil {
		q.plan.op_true.out[0] = entry
	}
	return .Ok
}

@(private = "file")
_union :: proc(sql: ^Streamql, q: ^Query) -> Result {
	if len(q.unions) == 0 {
		return .Ok
	}
	return not_implemented()
}

@(private = "file")
_order :: proc(sql: ^Streamql, q: ^Query) -> Result {
	return not_implemented()
}

/* In an effort to make building of the process graph easier
 * passive nodes are used as a sort of link between the steps.
 * Here, we *attempt* to remove the passive nodes and bridge
 * the gaps between.
 */
@(private = "file")
_clear_passive :: proc(p: ^Plan) {
	p := p // TODO: remove
	for n in &p.proc_graph.nodes {
		for n.out[0] != nil {
			if .Is_Passive not_in n.out[0].data.state {
				break
			}
			n.out[0] = n.out[0].out[0]
		}
		for n.out[1] != nil {
			if .Is_Passive not_in n.out[1].data.state {
				break
			}
			/* This has to be wrong... but it works... */
			if n.out[1].out[1] == nil {
				n.out[1] = n.out[1].out[1]
			} else {
				n.out[1] = n.out[1].out[0]
			}
		}
	}

	nodes := &p.proc_graph.nodes

	for i := 0; i < len(nodes); /* no advance */ {
		if .Is_Passive in nodes[i].data.state {
			process_destroy(&nodes[i].data)
			bigraph.remove(&p.proc_graph, nodes[i])
		} else {
			i += 1
		}
	}
}

@(private = "file")
_stranded_roots_for_delete :: proc(p: ^Plan) {
	for root in &p.proc_graph.roots {
		if root == p.op_false || root == p.op_true {
			continue
		}

		if root.out[0] == nil && root.out[1] == nil {
			root.data.state += {.Is_Passive}
			p.op_false.data.state -= {.Wait_In0}
			p.op_false.data.state += {.In0_Always_Dead}
		}
	}

	_clear_passive(p)
	bigraph.set_roots(&p.proc_graph)
}

@(private = "file")
_mark_roots_const :: proc(roots: ^[dynamic]^bigraph.Node(Process), id: u8) {
	for root in roots {
		pr := &root.data
		if pr.plan_id != id {
			continue
		}
		if pr.action__  != sql_read {
			if .Root_Fifo0 not_in pr.state && .Root_Fifo1 not_in pr.state {
				pr.state += {.Root_Fifo0}
			}
			pr.state += {.Is_Const}
		}
	}
}

@(private = "file")
_all_roots_are_const :: proc(roots: ^[dynamic]^bigraph.Node(Process)) -> bool {
	for root in roots {
		if .Is_Const not_in root.data.state {
			return false
		}
	}
	return true
}

@(private = "file")
_search_and_mark_const_selects :: proc(q: ^Query) -> bool {
	return false
}

_get_union_pipe_count :: proc(nodes: ^[dynamic]^bigraph.Node(Process)) -> int {
	total := 0
	for node in nodes {
		total += len(node.data.union_data.n)
	}
	return total
}

@(private = "file")
_activate_procs :: proc(sql: ^Streamql, q: ^Query) {
	graph_size := len(q.plan.proc_graph.nodes)
	union_pipes := _get_union_pipe_count(&q.plan.proc_graph.nodes)
	proc_count := graph_size + union_pipes
	fifo_base_size := proc_count * int(sql.pipe_factor)

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

	_preempt(&q.plan)

	for node in &q.plan.proc_graph.nodes {
		node.data.root_fifo_ref = &q.plan.root_fifos
	}

	if sql.verbosity == .Debug {
		fmt.eprintf("processes: %d\npipes: %d\nroot size: %d\n", proc_count, pipe_count, root_size)
	}
}

@(private = "file")
_make_pipes :: proc(p: ^Plan) {
	for n in &p.proc_graph.nodes {
		if n.out[0] != nil {
			proc0 := n.out[0].data
			if .Is_Dual_Link in n.out[0].data.state {
				n.data.output[0] = proc0.input[0]
				n.data.output[1] = proc0.input[1]
				continue
			}
			n.data.output[0] = .Is_Secondary in n.data.state ? proc0.output[1] : proc0.input[0]
		}

		if n.out[1] != nil {
			proc1 := n.out[1].data
			if .Is_Dual_Link in n.out[0].data.state {
				n.data.output[0] = proc1.input[0]
				n.data.output[1] = proc1.input[1]
				continue
			}
			n.data.output[1] = .Is_Secondary in n.data.state ? proc1.output[1] : proc1.input[0]
		}
	}
}

@(private = "file")
_update_pipes :: proc(g: ^bigraph.Graph(Process)) {
	for bigraph.traverse(g) != nil {}

	for n in g.nodes {
		assert(n.visit_count > 0)
		n.data.input[0].input_count = u8(n.visit_count)
		if n.data.input[1] != nil {
			n.data.input[1].input_count = u8(n.visit_count)
		}
	}

	bigraph.reset(g)
}

@(private = "file")
_calculate_execution_order :: proc(p: ^Plan) {
	p.execute_vector = make([]Process, len(p.proc_graph.nodes))

	node := bigraph.traverse(&p.proc_graph)
	for i := 0; node != nil; i += 1 {
		p.execute_vector[i] = node.data
		node = bigraph.traverse(&p.proc_graph)
	}

	//new_wait_list: []^Process = make([]^Process, len(p.proc_graph.nodes))

}

@(private = "file")
_build :: proc(sql: ^Streamql, q: ^Query, entry: ^bigraph.Node(Process) = nil, is_union: bool = false) -> Result {
	for subq in &q.subquery_exprs {
		_build(sql, subq) or_return
	}

	q.plan = make_plan()

	_from(sql, q) or_return
	_where(sql, q) or_return
	_group(sql, q) or_return
	_having(sql, q) or_return
	_operation(sql, q, entry, is_union) or_return

	//_print(&q.plan)

	_clear_passive(&q.plan)
	bigraph.set_roots(&q.plan.proc_graph)
	_union(sql, q) or_return
	_order(sql, q) or_return
	_clear_passive(&q.plan)
	bigraph.set_roots(&q.plan.proc_graph)

	_stranded_roots_for_delete(&q.plan)

	if len(q.plan.proc_graph.nodes) == 0 {
		if entry != nil {
			entry.data.state += {.Is_Const}
		}
		return .Ok
	}

	for subq in &q.subquery_exprs {
		bigraph.consume(&q.plan.proc_graph, &subq.plan.proc_graph)
		//destroy_plan(&subq.plan)
	}

	bigraph.set_roots(&q.plan.proc_graph)

	if _all_roots_are_const(&q.plan.proc_graph.roots) {
		q.plan.state += {.Is_Const}
	}
	_search_and_mark_const_selects(q)

	/* Only non-subqueries beyond this point */
	if q.sub_id != 0 {
		return .Ok
	}

	_activate_procs(sql, q)
	_make_pipes(&q.plan)
	_update_pipes(&q.plan.proc_graph)
	_calculate_execution_order(&q.plan)

	return .Ok
}

@(private = "file")
_print_col_sep :: proc(w: ^bufio.Writer, n: int) {
	for n := n; n > 0; n -= 1 {
		bufio.writer_write_byte(w, ' ')
	}
	bufio.writer_write_byte(w, '|')
}

@(private = "file")
_print :: proc(p: ^Plan) {
	io_w, ok := io.to_writer(os.stream_from_handle(os.stderr))
	if !ok {
		fmt.eprintln("_print failure")
		return
	}
	w: bufio.Writer
	bufio.writer_init(&w, io_w)

	max_len := len("BRANCH 0")

	for n in p.proc_graph.nodes {
		if len(n.data.msg) > max_len {
			max_len = len(n.data.msg)
		}
	}

	max_len += 1

	/* Print header */
	bufio.writer_write_string(&w, "NODE")
	_print_col_sep(&w, max_len - len("NODE"))
	bufio.writer_write_string(&w, "BRANCH 0")
	_print_col_sep(&w, max_len - len("BRANCH 0"))
	bufio.writer_write_string(&w, "BRANCH 1")
	bufio.writer_write_byte(&w, '\n')

	for i := 0; i < max_len; i += 1 {
		bufio.writer_write_byte(&w, '=')
	}
	_print_col_sep(&w, 0)
	for i := 0; i < max_len; i += 1 {
		bufio.writer_write_byte(&w, '=')
	}
	_print_col_sep(&w, 0)
	for i := 0; i < max_len; i += 1 {
		bufio.writer_write_byte(&w, '=')
	}

	for n in p.proc_graph.nodes {
		bufio.writer_write_byte(&w, '\n')
		bufio.writer_write_string(&w, n.data.msg)
		_print_col_sep(&w, max_len - len(n.data.msg))

		length := 0
		if n.out[0] != nil {
			bufio.writer_write_string(&w, n.out[0].data.msg)
			length = len(n.out[0].data.msg)
		}
		_print_col_sep(&w, max_len - length)
		if n.out[1] != nil {
			bufio.writer_write_string(&w, n.out[1].data.msg)
		}
	}
	bufio.writer_write_byte(&w, '\n')
	bufio.writer_flush(&w)
	bufio.writer_destroy(&w)
}
//+private

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
	_in_buf_iedx: u32,
	max_iters: u16,
	state: bit_set[Process_State],
	plan_id: u8,
	in_src_count: u8,
	out_src_count: u8,
}

make_process :: proc(plan: ^Plan, msg: string) -> Process {
	if plan == nil {
		return Process {
			msg = strings.clone(msg),
			state = {.Is_Enabled, .Wait_In0},
			max_iters = bits.U16_MAX,
		}
	}
	return Process {
		msg = strings.clone(msg),
		plan_id = plan.id,
		in_src_count = plan.src_count,
		out_src_count = plan.src_count,
		state = {.Is_Enabled, .Wait_In0},
		max_iters = bits.U16_MAX,
	}
}

process_destroy :: proc(process: ^Process) {
	delete(process.msg)
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
		if process.action__ == sql_groupby && .Is_Const in process.state {
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

process_enable :: proc(process: ^Process) {
	not_implemented()
}
process_disable :: proc(process: ^Process) {
	not_implemented()
}
process_add_to_wait_list :: proc(waiter: ^Process, waitee: ^Process) {
	not_implemented()
}
//+private

Query :: struct {
	operation: Select,
	plan: Plan,
	unions: [dynamic]^Query,
	subquery_exprs: [dynamic]^Query,
	var_source_vars: [dynamic]i32,
	var_sources: [dynamic]i32,
	var_expr_vars: [dynamic]i32,
	into_table_name: string,
	preview_text: string,
	top_count: i64,
	next_idx_ref: ^u32,
	next_idx: u32,
	idx: u32,
	into_table_var: i32,
	union_id: i32,
	sub_id: i16,
	query_total: i16,
}

//+private


Record :: struct {
	fields: []string,
	offset: i64,
	idx: i64,
	next: ^Record,
	ref: ^Record,
	select_len: i32,
	ref_count: i16,
	root_fifo_idx: u8,
	src_idx: u8,
}

destroy_record :: proc(rec: ^Record) {

}

record_get :: proc(rec: ^Record, src_idx: u8) -> ^Record {
	rec := rec
	for ; rec != nil; rec = rec.next {
		if rec.src_idx == src_idx {
			return rec
		}
	}
	return nil
}

record_get_line :: proc(rec: ^Record) -> string {
	return ""
}
//+private

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

Schema :: struct {
	layout: [dynamic]Schema_Item,
	name: string,
	schema_path: string,
	delim: string,
	rec_term: string,
	props: bit_set[Schema_Props],
}
//+private


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

make_select :: proc() -> Select {
	return Select {
		select__ = _select,
		select_list = make([dynamic]^Select),
		select_idx = -1,
	}
}

select_reset :: proc(s: ^Select) -> Result {
	s.offset = 0
	s.row_num = 0
	s.rows_affected = 0

	if len(s.select_list) != 0 {
		s.select_idx = 0
	}

	return .Ok
}

select_preop :: proc(sql: ^Streamql, s: ^Select, q: ^Query) -> Result {
	if len(s.select_list) != 0 {
		s.select_idx = 0
	}

	return not_implemented()
}


select_apply_process :: proc(q: ^Query, is_subquery: bool) {
	sel := &q.operation
	process := &q.plan.op_true.data
	process.action__ = sql_select
	process.data = sel

		sel.select__ = _select_subquery

	/* Build plan description */
	b := strings.make_builder()
	strings.write_string(&b, "SELECT ")

	first := true

	process.msg = strings.to_string(b)

	process = &q.plan.op_false.data
	process.state += {.Is_Passive}
}

select_next_union :: proc(sel: ^Select) -> bool {
	sel.select_idx += 1
	return int(sel.select_idx) < len(sel.select_list)
}

select_verify_must_run :: proc(sel: ^Select) {
	if .Must_Run_Once in sel.schema.props {
		sel.schema.props -= {.Must_Run_Once}
	}
}

_select :: proc(sel: ^Select, recs: ^Record) -> Process_Result {
	sel.row_num += 1


	if recs == nil {
		return .Ok
	}

	recs.offset = sel.offset
	return .Ok
}

_select_api :: proc(sel: ^Select, recs: ^Record) -> Process_Result {
	not_implemented()
	return .Error
}

_select_order_api:: proc(sel: ^Select, recs: ^Record) -> Process_Result {
	not_implemented()
	return .Error
}

_select_to_const :: proc(sel: ^Select, recs: ^Record) -> Process_Result {
	not_implemented()
	return .Error
}

_select_to_list :: proc(sel: ^Select, recs: ^Record) -> Process_Result {
	not_implemented()
	return .Error
}

_select_subquery :: proc(sel: ^Select, recs: ^Record) -> Process_Result {
	not_implemented()
	return .Error
}
//+private

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
		root_fifo := &_p.root_fifo_ref[recs.root_fifo_idx]
		if recs.ref != nil {
			if recs.ref.ref_count - 1 == 0 {
				sqlprocess_recycle(_p, recs.ref)
			} else {
				recs.ref.ref_count -= 1
			}
			recs.ref = nil
		}

		recs.ref_count -= 1
		next_rec := recs.next
		recs.next = nil
		if recs.ref_count == 0 {
			recs.ref_count = 1
			fifo.add(root_fifo, recs)
		}

		recs = next_rec
	}
}

sql_read :: proc(_p: ^Process) -> Process_Result {
	not_implemented()
	return .Error
}

sql_cartesian_join :: proc(_p: ^Process) -> Process_Result {
	not_implemented()
	return .Error
}

sql_hash_join :: proc(_p: ^Process) -> Process_Result {
	not_implemented()
	return .Error
}

sql_left_join_logic :: proc(_p: ^Process) -> Process_Result {
	not_implemented()
	return .Error
}

sql_logic :: proc(_p: ^Process) -> Process_Result {
	not_implemented()
	return .Error
}

sql_groupby :: proc(_p: ^Process) -> Process_Result {
	not_implemented()
	return .Error
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
		if .Must_Run_Once in main_select.schema.props {
			main_select.select__(main_select, nil) or_return
			main_select.rows_affected += 1
			_p.rows_affected += 1
			return .Running
		}

		/* subquery reads expect union schema to
		 * be "in sync" with the subquery select's
		 * current schema
		 */
		if out != nil && !fifo.is_empty(out) {
			return .Running
		}

		if select_next_union(main_select) {
			_p.state += {.Wait_In0}
			fifo.set_open(in_, false)
			/* QUEUED RESULTS */
			//return .Running
			not_implemented()
			return .Error
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
		if iters >= _p.max_iters || main_select.rows_affected >= current_select.top_count {
			res = .Running
			break
		}

		main_select.select__(main_select, recs) or_return

		_p.rows_affected += 1
		main_select.rows_affected += 1

		if out != nil {
			fifo.add(out, recs)
		} else if .Is_Const in current_select.schema.props {
			sqlprocess_recycle(_p, recs)
		}

		if .Is_Const in current_select.schema.props {
			_p.state -= {.Wait_In0}
			res = .Running
			break
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

PIPE_MIN :: 2
PIPE_MAX :: 1024
PIPE_DEFAULT :: 16
PIPE_DEFAULT_THREAD :: 64

Config :: enum u8 {
	Check,
	Strict,
	Thread,
	Overwrite,
	Summarize,
	Parse_Only,
	Print_Plan,
	Force_Cartesian,
	Add_Header,
	No_Header, /* lol? */
	_Allow_Stdin,
	_Delim_Set,
	_Rec_Term_Set,
	_Schema_Paths_Resolved,
}

Verbose ::enum u8 {
	Quiet,
	Basic,
	Noisy,
	Debug,
}

Quotes :: enum u8 {
	None,
	Weak,
	Rfc4180,
	All,
}

Result :: enum u8 {
	Ok,
	Running,
	Error,
	Eof,
	Null, // refering to NULL in SQL
}

@private
_Branch_State :: enum u8 {
	No_Branch,
	Expect_Expr,
	Expect_Else,
	Expect_Exit,
}

Streamql :: struct {
	default_schema: string,
	schema_map: map[string]^Schema,
	schema_paths: [dynamic]string,
	queries: [dynamic]^Query,
	in_delim: string,
	out_delim: string,
	rec_term: string,
	pipe_factor: u32,
	config: bit_set[Config],
	branch_state: _Branch_State,
	in_quotes: Quotes,
	out_quotes: Quotes,
	verbosity: Verbose,
}

construct :: proc(sql: ^Streamql, cfg: bit_set[Config] = {}) {
	sql^ = {
		schema_paths = make([dynamic]string),
		queries = make([dynamic]^Query),
		config = cfg,
	}

	sql.pipe_factor = .Thread in sql.config ? PIPE_DEFAULT_THREAD : PIPE_DEFAULT

}

destroy :: proc(sql: ^Streamql) {
}

generate_plans :: proc(sql: ^Streamql, query_str: string) -> Result {
	if plan_build(sql) == .Error {
		reset(sql)
		return .Error
	}
	if .Print_Plan in sql.config {
		plan_print(sql)
	}
	return .Ok
}

exec_plans :: proc(sql: ^Streamql, limit: int = bits.I32_MAX) -> Result {
	res: Result

	i := 0

	for ; i < len(sql.queries); i += 1 {
		if .Has_Stepped in sql.queries[i].plan.state {
			fmt.eprintln("Cannot execute plan that has stepped")
			return .Error
		}

		if sql.verbosity > Verbose.Basic {
			fmt.printf("EXEC: %s\n", sql.queries[i].preview_text)
		}

		if res == .Error {
			break
		}

		if sql.verbosity > Verbose.Quiet {
			_print_footer(sql.queries[i])
		}
		i = int(sql.queries[i].next_idx)
	}

	if res == .Error || i >= len(sql.queries) {
		reset(sql)
	}

	return res
}

exec :: proc(sql: ^Streamql, query_str: string) -> Result {
	generate_plans(sql, query_str) or_return
	if .Check in sql.config {
		reset(sql)
		return .Ok
	}

	return exec_plans(sql)
}

reset :: proc(sql: ^Streamql) {
	clear(&sql.queries)
}

add_schema_path :: proc(sql: ^Streamql, path: string, throw: bool = true) -> Result {
	if !os.is_dir(path) {
		if throw {
			fmt.eprintf("`%s' does not appear to be a directory\n", path)
		}
		return .Error
	}

	append(&sql.schema_paths, strings.clone(path))
	return .Ok
}

@private
not_implemented :: proc(loc := #caller_location) -> Result {
	fmt.fprintln(os.stderr, "not implemented:", loc)
	return .Error
}

_print_footer :: proc(q: ^Query) {
	not_implemented()
}
