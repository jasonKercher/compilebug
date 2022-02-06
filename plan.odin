//+private
package streamql

import "core:strings"
import "core:bufio"
import "core:fmt"
import "core:io"
import "core:os"
import "bigraph"
import "fifo"

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
_make_join_proc :: proc(p: ^Plan, type: Join_Type, algo_str: string) -> Process {
	msg: string
	#partial switch type {
	case .Inner:
		msg = fmt.tprintf("INNER JOIN (%s)", algo_str)
	case .Left:
		msg = fmt.tprintf("LEFT JOIN (%s)", algo_str)
	case .Right:
		msg = fmt.tprintf("RIGHT JOIN (%s)", algo_str)
	case .Full:
		msg = fmt.tprintf("FULL JOIN (%s)", algo_str)
	case .Cross:
		msg = fmt.tprintf("CROSS JOIN (%s)", algo_str)
	}

	return make_process(p, msg)
}

@(private = "file")
_subquery_inlist :: proc(p: ^Plan, lg: ^Logic_Group, logic_proc: ^Process) -> Result {
	return not_implemented()
}

@(private = "file")
_check_for_special_expr :: proc(p: ^Plan, process: ^Process, expr: ^Expression) {
	#partial switch v in &expr.data {
	case Expr_Subquery:
		process_add_to_wait_list(process, &v.plan.op_true.data)
	case Expr_Case:
		not_implemented()
	case Expr_Aggregate:
		_check_for_special_exprs(p, process, &v.args)
	case Expr_Function:
		_check_for_special_exprs(p, process, &v.args)
	}
}

@(private = "file")
_check_for_special_exprs :: proc(p: ^Plan, process: ^Process, exprs: ^[dynamic]Expression) {
	if exprs == nil {
		return
	}
	exprs := exprs
	for e in exprs {
		_check_for_special_expr(p, process, &e)
	}
}

_check_for_special :: proc{_check_for_special_expr, _check_for_special_exprs}

@(private = "file")
_logic_to_process :: proc(p: ^Plan, logic_proc: ^Process, lg: ^Logic_Group, sb: ^strings.Builder) -> Result {
	switch lg.type {
	case .And:
		strings.write_string(sb, "AND(")
	case .Or:
		strings.write_string(sb, "OR(")
	case .Not:
		strings.write_string(sb, "NOT(")
	case .Predicate_Negated:
		strings.write_string(sb, "NOT ")
		fallthrough
	case .Predicate:
		if int(lg.condition.comp_type) >= int(Comparison.Sub_In) {
			_subquery_inlist(p, lg, logic_proc) or_return
		}
		_check_for_special(p, logic_proc, &lg.condition.exprs[0])
		_check_for_special(p, logic_proc, &lg.condition.exprs[1])
		logic_assign_process(lg.condition, logic_proc) or_return
	}

	if lg.items[0] != nil {
		_logic_to_process(p, logic_proc, lg.items[0], sb) or_return
	}
	if lg.items[1] != nil {
		_logic_to_process(p, logic_proc, lg.items[1], sb) or_return
	}

	strings.write_byte(sb, ')')
	return .Ok
}

@(private = "file")
_insert_logic_proc :: proc(p: ^Plan, lg: ^Logic_Group, is_hash_join: bool = false) -> (ptr: ^Process, res: Result) {
	logic_proc := make_process(p, "")
	logic_proc.action__ = sql_logic
	logic_proc.data = lg

	sb := strings.make_builder()
	_logic_to_process(p, &logic_proc, lg, &sb) or_return
	logic_proc.msg = strings.to_string(sb)

	logic_node := bigraph.add(&p.proc_graph, logic_proc)
	
	if is_hash_join {
		p.curr.out[1] = logic_node
		if logic_group_get_condition_count(lg) == 1 {
			logic_node.data.state += {.Is_Passive}
		}
	} else {
		p.curr.out[0] = logic_node
	}

	logic_node.out[0] = p.op_false

	logic_true_proc := make_process(nil, "logic true")
	logic_true_proc.state += {.Is_Passive}
	logic_true_node := bigraph.add(&p.proc_graph, logic_true_proc)
	logic_node.out[1] = logic_true_node
	p.curr = logic_true_node

	ptr = &logic_node.data
	return
}

@(private = "file")
_from :: proc(sql: ^Streamql, q: ^Query) -> Result {
	if len(q.sources) == 0 {
		return .Ok
	}

	from_node: ^bigraph.Node(Process)
	p := &q.plan

	switch v in &q.sources[0].data {
	case string: /* File Source */
		msg := fmt.tprintf("%s: stream read", v)
		from_proc := make_process(p, msg)
		from_proc.state += {.Root_Fifo0}
		from_proc.action__ = sql_read
		from_proc.data = &q.sources[0]
		from_node = bigraph.add(&p.proc_graph, from_proc)
		from_node.is_root = true
	case ^Query: /* Subquery Source */
		from_proc := make_process(p, "subquery source1")
		from_proc.action__ = sql_read
		from_proc.data = &q.sources[0]
		from_node = bigraph.add(&p.proc_graph, from_proc)
		_build(sql, q, from_node) or_return
		bigraph.consume(&p.proc_graph, &v.plan.proc_graph)
		destroy_plan(&v.plan)
	}
	/* NOTE: This next line may be incorrect */
	//q.sources[0].read_proc = from_proc

	p.curr.out[0] = from_node
	p.curr = from_node

	for src, i in &q.sources {
		if i == 0 {
			continue
		}
		join_proc: Process
		join_node: ^bigraph.Node(Process)
		is_hash_join := src.join_data != nil
		if is_hash_join {
			join_proc = _make_join_proc(p, src.join_type, "hash")
			join_proc.action__ = sql_hash_join
			join_proc.state += {.Has_Second_Input}
			join_proc.data = &src

			if .Is_Stdin in src.props {
				reader_start_file_backed_input(&src.schema.data.(Reader))
			}
			hash_join_init(&src)
			
			read_node: ^bigraph.Node(Process)
			switch v in &src.data {
			case string: /* File Source */
				msg: string
				if .Is_Stdin in src.props {
					msg = "file-backed read stdin"
				} else {
					msg = fmt.tprintf("%s: random access", v)
				}
				read_proc := make_process(p, msg)
				read_proc.state += {.Root_Fifo0, .Is_Secondary}
				read_proc.action__ = sql_read
				read_proc.data = &src
				read_node = bigraph.add(&p.proc_graph, read_proc)
				read_node.is_root = true
			case ^Query: /* Subquery */
				subquery_start_file_backed_input(&src.schema.data.(Reader))
				read_proc := make_process(p, "file-backed read subquery")
				read_proc.state += {.Is_Secondary}
				read_proc.action__ = sql_read
				read_proc.data = &src
				read_node = bigraph.add(&q.plan.proc_graph, read_proc)
				_build(sql, v, read_node) or_return
				bigraph.consume(&q.plan.proc_graph, &v.plan.proc_graph)
				// destroy_plan(&v.plan)
			}

			join_node := bigraph.add(&q.plan.proc_graph, join_proc)
			read_node.out[0] = join_node
		} else { /* Cartesian */
			if src.join_type != .Inner {
				fmt.eprintf("cartesian join only works with INNER JOIN\n")
				return .Error
			}
			if _, is_subquery := src.data.(^Query); is_subquery {
				fmt.eprintf("subquery invalid on the right side of cartesian join\n")
				return .Error
			}
			fmt.eprintf("Warning: slow cartesian join detected\n")
			join_proc := _make_join_proc(&q.plan, src.join_type, "cartesian")
			join_proc.state += {.Root_Fifo1}
			join_proc.data = &src
			join_node := bigraph.add(&q.plan.proc_graph, join_proc)
			join_node.is_root = true
		}

		q.plan.curr.out[0] = join_node
		q.plan.curr = join_node

		if src.join_logic != nil {
			logic_proc := _insert_logic_proc(&q.plan, src.join_logic, is_hash_join) or_return
			if src.join_type == .Left {
				logic_proc.action__ = sql_left_join_logic
			}
		}
	}
	return .Ok
}

@(private = "file")
_where :: proc(sql: ^Streamql, q: ^Query) -> Result {
	if q.where_ == nil {
		return .Ok
	}
	_, res := _insert_logic_proc(&q.plan, q.where_)
	return res
}

@(private = "file")
_group :: proc(sql: ^Streamql, q: ^Query) -> Result {
	if q.groupby == nil {
		return .Ok
	}
	return not_implemented()
}

@(private = "file")
_having :: proc(sql: ^Streamql, q: ^Query) -> Result {
	if q.having == nil {
		return .Ok
	}
	_, res := _insert_logic_proc(&q.plan, q.having)
	return res
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

	op_apply_process(q, is_union || entry != nil) or_return
	_check_for_special(&q.plan, &q.plan.op_true.data, op_get_expressions(&q.operation))

	op_add_exprs := op_get_additional_expressions(&q.operation)
	_check_for_special(&q.plan, &q.plan.op_true.data, op_add_exprs)

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
	if q.orderby == nil {
		return .Ok
	}
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
	select, is_select := &q.operation.(Select)
	is_const := true

	for src in q.sources {
		if subq, is_subq := src.data.(^Query); is_subq {
			sub_is_const := _search_and_mark_const_selects(subq)
			if !sub_is_const {
				is_const = false
			}
		} else {
			is_const = false
		}
	}

	if is_select && is_const  {
		select.schema.props += {.Is_Const}
	}

	return is_const
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
	if len(q.sources) == 0 {
		_mark_roots_const(&q.plan.proc_graph.roots, q.plan.id)
	}

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
