//+private
package streamql

import "core:fmt"
import "fifo"

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

		if writer_close(&main_select.schema.data.(Writer)) == .Error {
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
