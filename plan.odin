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
_from :: proc(sql: ^Streamql, q: ^Query) -> Result {
	if len(q.sources) == 0 {
		return .Ok
	}
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
