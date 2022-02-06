//+private
package streamql

import "core:path/filepath"
import "core:math/bits"
import "core:strings"
import "core:fmt"
import "fastrecs"
import "core:os"
import "bytemap"
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
	item_map: bytemap.Multi(i32),
	name: string,
	schema_path: string,
	delim: string,
	rec_term: string,
	props: bit_set[Schema_Props],
	write_io: Io,
	io: Io,
}

make_schema :: proc() -> Schema {
	return Schema {
		props = {.Is_Default, .Must_Run_Once},
	}
}

destroy_schema :: proc(s: ^Schema) {
	delete(s.delim)
	delete(s.rec_term)
	for item in s.layout {
		delete(item.name)
	}
	delete(s.layout)
	bytemap.destroy(&s.item_map)
}

schema_eq :: proc(s1: ^Schema, s2: ^Schema) -> bool {
	not_implemented()
	return true
}

schema_copy :: proc(dest: ^Schema, src: ^Schema) {
	if src == nil {
		if .Delim_Set not_in dest.props {
			schema_set_delim(dest, ",")
		}
		dest.io = .Delimited
		dest.write_io = .Delimited
		dest.props += {.Is_Default}
		return
	}

	if .Delim_Set not_in src.props {
		schema_set_delim(dest, src.delim)
	}

	dest.write_io = src.write_io
	if src.io == nil {
		dest.io = src.write_io
	} else {
		dest.io = src.io
	}
	if .Is_Default in src.props {
		dest.props += {.Is_Default}
	} else {
		dest.props -= {.Is_Default}
	}
}

schema_get_item :: proc(s: ^Schema, key: string) -> (Schema_Item, Result) {
	indices, found := bytemap.get(&s.item_map, key)
	if !found {
		return Schema_Item { loc = -1 }, .Ok
	}
	if len(indices) > 1 {
		fmt.eprintf("expression `%s' ambiguous\n", key)
		return Schema_Item { loc = -1 }, .Error
	}
	return s.layout[indices[0]], .Ok
}

schema_resolve :: proc(sql: ^Streamql) -> Result {
	_resolve_schema_paths(sql) or_return
	for q in &sql.queries {
		if q.next_idx_ref != nil {
			q.next_idx = q.next_idx_ref^
			q.next_idx_ref = nil
		}

		if ._Delim_Set in sql.config {
			op_set_delim(&q.operation, sql.out_delim)
		}

		if ._Rec_Term_Set in sql.config {
			op_set_rec_term(&q.operation, sql.rec_term)
		}

		_resolve_query(sql, q) or_return
	}

	return .Ok
}

schema_set_delim :: proc(s: ^Schema, delim: string) {
	if s == nil {
		return
	}
	delete(s.delim)
	s.delim = strings.clone(delim)
}

schema_set_rec_term :: proc(s: ^Schema, rec_term: string) {
	if s == nil {
		return
	}
	delete(s.rec_term)
	s.rec_term = strings.clone(rec_term)
}

schema_assign_header :: proc(src: ^Source, rec: ^Record, src_idx: int) {
	schema := &src.schema
	fr_rec := rec.data.(fastrecs.Record)
	for f in fr_rec.fields {
		new_item := Schema_Item {
			name = strings.clone(f),
			loc = i32(len(src.schema.layout)),
		}
		append(&src.schema.layout, new_item)
	}
	schema_preflight(schema)
}

schema_preflight :: proc(s: ^Schema) {
	//if s == nil {
	//	return
	//}

	/* May be called already from order.odin */
	if len(s.item_map.values) > 0 {
		return
	}

	s.item_map = bytemap.make_multi(i32, u64(len(s.layout) * 2), {.No_Case})

	for it, i in &s.layout {
		it.loc = i32(i)
		bytemap.set(&s.item_map, it.name, it.loc)
	}

	if .Delim_Set not_in s.props {
		schema_set_delim(s, ",");
	}

	if len(s.rec_term) == 0 {
		s.rec_term = "\n"
	}
}

@(private = "file")
_resolve_schema_paths :: proc(sql: ^Streamql) -> Result {
	/* Should only ever do this once */
	if ._Schema_Paths_Resolved in sql.config {
		return .Ok
	}
	sql.config += {._Schema_Paths_Resolved}

	/* SQL_SCHEMA_PATH */
	if path, ok := os.getenv("SQL_SCHEMA_PATH"); ok {
		add_schema_path(sql, path) or_return
	}

	/* $HOME/.config/streamql/schema */
	if home, ok := os.getenv("HOME"); ok {
		path := fmt.aprintf("%s/.config/streamql/schema/", home)
		add_schema_path(sql, path, false)
		delete(path)
	}

	/* TODO: figure out how to send ${datarootdir} */
	add_schema_path(sql, "/etc/streamql/schema/", false)

	return .Ok
}

@(private = "file")
_evaluate_if_const :: proc(expr: ^Expression) -> Result {
	fn := &expr.data.(Expr_Function)
	for expr in fn.args {
		if _, is_const := expr.data.(Expr_Constant); !is_const {
			return .Ok
		}
	}

	expr.fn_bak = new(Expr_Function)
	expr.fn_bak^ = fn^

	new_data: Data
	fn.call__(fn, &new_data, nil) or_return
	expr.data = Expr_Constant(new_data)

	return .Ok
}

@(private = "file")
_try_assign_source :: proc(col: ^Expr_Column_Name, src: ^Source, src_idx: int) -> int {
	src := src
	indices, ok := bytemap.get(&src.schema.item_map, col.item.name)
	if !ok {
		return 0
	}

	first_match := src.schema.layout[indices[0]]
	expression_link(col, first_match, src_idx, src)
	return len(indices)
}

@(private = "file")
_assign_expression :: proc(expr: ^Expression, sources: []Source, strict: bool = true) -> Result {
	matches := 0
	sources := sources

	#partial switch v in &expr.data {
	case Expr_Case:
		return not_implemented()
	case Expr_Function:
		_assign_expressions(&v.args, sources, strict) or_return
		function_op_resolve(expr, expr.data) or_return
		function_validate(&v, expr) or_return
		return _evaluate_if_const(expr)
	case Expr_Subquery:
		return select_resolve_type_from_subquery(expr)
	case Expr_Full_Record:
		if i32(v) != -1 {
			return .Ok
		}

		for src, i in &sources {
			if expr.table_name == "" || expr.table_name == src.alias {
				v = Expr_Full_Record(i)
				matches += 1
			}
		}
	case Expr_Column_Name:
		if v.item.loc != -1 {
			return .Ok
		}

		for src, i in &sources {
			n : int
			if expr.table_name == "" || expr.table_name == src.alias {
				n = _try_assign_source(&v, &src, i)
				//if n > 0 {
				//	v.src_idx = i32(i)
				//}
				if n > 1 && !strict {
					n = 1
				}
				matches += n
			}
		}
	case:
		return .Ok
	}

	if matches > 1 {
		fmt.eprintf("ambiguous expression: `%s'\n", expr.alias)
		return .Error
	}

	if matches == 0 {
		fmt.eprintf("cannot find expression: `%s'\n", expr.alias)
		return .Error
	}
	return .Ok
}

@(private = "file")
_assign_expressions :: proc(exprs: ^[dynamic]Expression, sources: []Source, strict: bool = true) -> Result {
	if exprs == nil {
		return .Ok
	}
	exprs := exprs
	for e in exprs {
		_assign_expression(&e, sources, strict) or_return
	}
	return .Ok
}

@(private = "file")
_assign_logic_group_expressions :: proc(lg: ^Logic_Group, sources: []Source, strict: bool = true) -> Result {
	if lg == nil {
		return .Ok
	}

	switch lg.type {
	case .And:
		fallthrough
	case .Or:
		_assign_logic_group_expressions(lg.items[0], sources, strict) or_return
		return _assign_logic_group_expressions(lg.items[1], sources, strict)
	case .Not:
		return _assign_logic_group_expressions(lg.items[0], sources, strict)
	case .Predicate:
		fallthrough
	case .Predicate_Negated:
		_assign_expression(&lg.condition.exprs[0], sources, strict) or_return
		return _assign_expression(&lg.condition.exprs[1], sources, strict)
	}

	return .Ok
}

@(private = "file")
_load_schema_by_name :: proc(sql: ^Streamql, src: ^Source, src_idx: int) -> Result {
	return not_implemented()
}

@(private = "file")
_resolve_file :: proc(sql: ^Streamql, q: ^Query, src: ^Source) -> Result {
	if .Is_Stdin in src.props {
		return .Ok
	}

	table_name := src.data.(string)
	r := &src.schema.data.(Reader)

	/* Must match the file name exactly in strict mode */
	if .Strict in sql.config {
		if !os.is_file(table_name) {
			fmt.eprintf("table not found: `%s'\n", table_name)
			return .Error
		}
		r.file_name = table_name
	} else {
		file_name, fuzzy_res := util.fuzzy_file_match(table_name)
		switch fuzzy_res {
		case .Ambiguous:
			fmt.eprintf("table name ambiguous: `%s'\n", table_name)
			return .Error
		case .Not_Found:
			fmt.eprintf("table not found: `%s'\n", table_name)
			return .Error
		case .Found:
		}
		r.file_name = file_name
	}

	full_path, ok := filepath.abs(r.file_name, context.temp_allocator)
	if !ok {
		fmt.eprintf("failed to find absolute path for `%s'\n", r.file_name)
		return .Error
	}
	match_schema, found := sql.schema_map[full_path]
	if !found {
		return .Ok
	}

	/* At this point we can assume we are reading from a file that
	 * will have been modified by the time we try to read from it.
	 * So we must use the schema it *will* have.
	 */
	assert(len(src.schema.layout) == 0)
	append(&src.schema.layout, ..match_schema.layout[:])

	src.props += {.Must_Reopen}
	src.schema.props += {.Is_Preresolved}
	schema_copy(&src.schema, match_schema)
	r.type = match_schema.write_io

	return .Ok
}

@(private = "file")
_resolve_source :: proc(sql: ^Streamql, q: ^Query, src: ^Source, src_idx: int) -> Result {
	if len(src.schema.item_map.values) != 0 {
		return .Ok
	}

	src.schema.data = make_reader()
	r := &src.schema.data.(Reader)

	if src.schema.name == "" && sql.default_schema != "" {
		src.schema.name = strings.clone(sql.default_schema)
	}
	if src.schema.name != "" {
		/* TODO: case_insensitive */
		if src.schema.name != "default" {
			src.schema.props -= {.Is_Default}
			r.skip_rows = 0
			_load_schema_by_name(sql, src, src_idx) or_return
		}
	}

	switch v in src.data {
	case ^Query:
		//if src.alias == "" { throw for missing alias?? }
		_resolve_query(sql, v)
		select := v.operation.(Select)
		src.schema = select.schema
		src.schema.props += {.Is_Preresolved}
		r.type = .Subquery
	case string:
		_resolve_file(sql, q, src) or_return
		if .Is_Default in src.schema.props {
			r.type = .Delimited
		}
	}

	reader_assign(sql, src) or_return

	#partial switch r.type {
	case .Fixed:
		schema_preflight(&src.schema)
		return .Ok
	case .Subquery:
		return .Ok
	}

	rec: Record
	rec.data = fastrecs.Record{}
	r.max_field_idx = bits.I32_MAX
	r.get_record__(r, &rec)
	r.max_field_idx = 0

	if .Is_Stdin not_in src.props {
		r.reset__(r)
	}

	/* if we've made it this far, we want to try
	 * and determine schema by reading the top
	 * row of the file and assume a delimited
	 * list of field names.
	 */
	if .Is_Default in src.schema.props {
		if .Is_Preresolved not_in src.schema.props {
			schema_assign_header(src, &rec, src_idx)
		}
	} else {
		new_size := 1 if len(rec.fields) == 0 else len(rec.fields)
		for i := len(src.schema.layout); i >= new_size; i -= 1 {
			item := pop(&src.schema.layout)
			delete(item.name)
		}
	}

	schema_preflight(&src.schema)

	if .Is_Default in src.schema.props || .Is_Stdin in src.props {
		destroy_record(&rec)
	} else {
		r.first_rec = rec
	}

	return .Ok
}

@(private = "file")
_get_join_side :: proc(expr: ^Expression, right_idx: int) -> Join_Side {
	#partial switch v in &expr.data {
	case Expr_Full_Record:
		return int(v) < right_idx ? .Left : .Right
	case Expr_Column_Name:
		// TODO: subquery_src_idx??
		return int(v.src_idx) < right_idx ? .Left : .Right
	case Expr_Function:
		side0: Join_Side
		for e in &v.args {
			side1 := _get_join_side(&e, right_idx)
			if side0 == nil {
				side0 = side1
			} else if side1 == nil {
				continue
			} else if side0 != side1 {
				return .Mixed
			}
		}
		return side0
	}
	return nil
}

@(private = "file")
_resolve_join_conditions :: proc(right_src: ^Source, right_idx: int) {
	if right_src.join_logic == nil || len(right_src.joinable_logic) == 0 {
		return
	}

	for l in right_src.joinable_logic {
		side0 := _get_join_side(&l.exprs[0], right_idx)
		if side0 == .Mixed {
			continue
		}
		side1 := _get_join_side(&l.exprs[1], right_idx)
		if side1 == .Mixed || side0 == side1 {
			continue
		}
		if !logic_must_be_true(right_src.join_logic, l) {
			continue
		}

		right_src.join_logic.join_logic = l
		join := new_hash_join()
		if side0 == .Right {
			join.right_expr = &l.exprs[0]
			join.left_expr = &l.exprs[1]
		} else {
			join.right_expr = &l.exprs[1]
			join.left_expr = &l.exprs[0]
		}
		join.comp_type = data_determine_type(l.exprs[0].data_type, l.exprs[1].data_type)
		right_src.join_data = join
		break
	}
}

@(private = "file")
_resolve_unions :: proc(sql: ^Streamql, q: ^Query) -> Result {
	if len(q.unions) == 0 {
		return .Ok
	}
	return not_implemented()
}

@(private = "file")
_resolve_asterisk :: proc(exprs: ^[dynamic]Expression, sources: []Source) -> Result {
	sources := sources
	for i := 0; i < len(exprs); i += 1 {
		//idx := i
		if _, is_aster := exprs[i].data.(Expr_Asterisk); !is_aster {
			continue
		}

		matches: int
		for src, j in &sources {
			if exprs[i].table_name == "" || exprs[i].table_name == src.alias {
				if matches > 0 {
					new_expr := make_expression(Expr_Asterisk(j))
					i += 1
					insert_at(exprs, i, new_expr)
				} else {
					aster := exprs[i].data.(Expr_Asterisk)
					aster = Expr_Asterisk(j)
				}
				matches += 1
			}
		}
		if matches == 0 {
			fmt.eprintf("failed to locate table `%s'\n", exprs[i].table_name)
			return .Error
		}
	}
	return .Ok
}

@(private = "file")
_map_groups :: proc(sql: ^Streamql, q: ^Query) -> Result {
	return not_implemented()
}

@(private = "file")
_group_validate_having :: proc(q: ^Query, is_summarize: bool) -> Result {
	return not_implemented()
}

@(private = "file")
_group_validation :: proc(q: ^Query, exprs, op_exprs: ^[dynamic]Expression, is_summarize: bool) -> Result {
	return not_implemented()
}

@(private = "file")
_resolve_query :: proc(sql: ^Streamql, q: ^Query, union_io: Io = nil) -> Result {
	/* First, let's resolve any subquery expressions.
	 * These should be constant values and are not
	 * tied to any parent queries
	 */
	for sub in &q.subquery_exprs {
		_resolve_query(sql, sub, union_io)
	}

	is_strict := .Strict in sql.config

	/* Top expression */
	if q.top_expr != nil {
		_assign_expression(q.top_expr, nil, is_strict) or_return
		if _, is_const := q.top_expr.data.(Expr_Constant); !is_const {
			fmt.eprintf("Could not resolve TOP expression\n")
			return .Error
		}
		data := Data(q.top_expr.data.(Expr_Constant))
		val, is_int := data.(i64)
		if !is_int {
			fmt.eprintf("Input to TOP clause must be an integer\n")
			return .Error
		}
		if val < 0 {
			fmt.eprintf("Input to TOP clause cannot be negative\n")
			return .Error
		}

		q.top_count = val
	}

	/* If there is an order by, make sure NOT to send top_count
	 * to the operation. However, if there is a union, this does
	 * not apply.  If there is a union, the top count belongs to
	 * the operation (select can be assumed). The only goal is to
	 * make sure ALL the selected records are ordered
	 */
	if q.orderby != nil && len(q.unions) == 0 {
		q.orderby.top_count = q.top_count
	} else {
		op_set_top_count(&q.operation, q.top_count)
	}

	/* Now, we should verify that all sources
	 * exist and populate schemas.  As we loop, we
	 * resolve the expressions that are listed in join
	 * clauses because of the following caveat:
	 *
	 * SELECT *
	 * FROM T1
	 * JOIN T2 ON T1.FOO = T3.FOO -- Cannot read T3 yet!
	 * JOIN T3 ON T2.FOO = T3.FOO
	 */
	for src, i in &q.sources {
		_resolve_source(sql, q, &src, i) or_return
		source_resolve_schema(sql, &src) or_return

		if union_io != nil {
			op_get_schema(&q.operation).write_io = union_io
		} else {
			op_set_schema(&q.operation, &src.schema)
		}

		if src.join_logic != nil {
			_assign_logic_group_expressions(src.join_logic, q.sources[:i+1], is_strict) or_return
		}

		if i > 0 && .Force_Cartesian not_in sql.config {
			_resolve_join_conditions(&src, i)
		}
	}

	op_schema := op_get_schema(&q.operation)
	if op_schema != nil && op_schema.write_io == nil {
		op_set_schema(&q.operation, nil)
	}

	/* Where clause */
	_assign_logic_group_expressions(q.where_, q.sources[:], is_strict) or_return

	/* Validate operation expressions */
	op_exprs := op_get_expressions(&q.operation)
	if _, is_select := q.operation.(Select); is_select {
		_resolve_asterisk(op_exprs, q.sources[:]) or_return
	}
	_assign_expressions(op_exprs, q.sources[:], is_strict) or_return

	op_add_exprs := op_get_additional_expressions(&q.operation)
	_assign_expressions(op_add_exprs, q.sources[:], is_strict) or_return

	/* Validate HAVING expressions */
	_assign_logic_group_expressions(q.having, q.sources[:], is_strict) or_return

	/* Validate ORDER BY expressions */
	order_exprs: ^[dynamic]Expression
	if q.orderby != nil {
		order_preresolve(q.orderby, &q.operation.(Select), q.sources[:]) or_return
		/* may have changed in preresolve */
		order_exprs = &q.orderby.expressions
		_assign_expressions(order_exprs, q.sources[:], is_strict) or_return
	}

	/* Do GROUP BY last. There are less caveats having
	 * waited until everything else is resolved
	 */
	if q.groupby != nil {
		_map_groups(sql, q) or_return

		is_summarize := .Summarize in sql.config

		/* Now that we have mapped the groups, we must
		 * re-resolve each operation, HAVING and ORDER BY
		 * expression to a group
		 */
		_group_validation(q, op_exprs, nil, is_summarize) or_return
		_group_validate_having(q, is_summarize) or_return
		_group_validation(q, order_exprs, op_exprs, is_summarize) or_return
	}

	_resolve_unions(sql, q) or_return
	op_writer_init(sql, q) or_return

	if q.groupby == nil && q.orderby != nil {
		/* This is normally handled during group processing,
		 * but if there is no GROUP BY, just assign preresolved
		 * ORDER BY expressions.
		 */
		for e in order_exprs {
			expr_ref, is_ref := e.data.(Expr_Reference)
			if !is_ref {
				continue // ???????
			}
			// TODO
			//matched := op_exprs
		}
	}

	schema_preflight(op_schema)

	if q.into_table_name == "" {
		return .Ok
	}

	/* If this query will be writing changes to the file system,
	 * we need to be aware of this when parsing future queries.
	 * These are mapped as absolute paths. First check that the
	 * file exists. If it doesn't, create it now so that realpath
	 * works. Creating the file also has the (undesireable?) affect
	 * of making fuzzy file discovery possible on a file that did
	 * not previously exist. TODO
	 */
	if !os.is_file(q.into_table_name) {
		/* NOTE: We may enter this block for a number of reasons.
		 *       We will rely on open for catching errors.
		 */
		fd, err := os.open(q.into_table_name, os.O_WRONLY | os.O_CREATE | os.O_TRUNC, 0o664)
		if err != os.ERROR_NONE {
			fmt.eprintf("failed to create file `%s'", q.into_table_name)
			return .Error
		}
		err = os.close(fd)
		if err != os.ERROR_NONE {
			fmt.eprintf("failed to close file `%s'", q.into_table_name)
			return .Error
		}
	} else if _, is_sel := q.operation.(Select); is_sel && .Overwrite in sql.config {
		fmt.eprintf("cannot SELECT INTO: `%s' already exists\n", q.into_table_name)
		return .Error
	}
	path, err := os.absolute_path_from_relative(q.into_table_name)
	if err == 0 {
		fmt.eprintln("failed to get absolute path")
		return .Error
	}

	sql.schema_map[path] = op_schema

	return .Ok
}

