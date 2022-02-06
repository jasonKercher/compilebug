//+private
package streamql

import "bytemap"

import "core:fmt"
import "core:strings"
import "core:math/bits"
import "core:container/bit_array"
import "core:testing"

PREVIEW_MAX :: 30

Parser :: struct {
	text:    string,
	lf_vec:  [dynamic]u32,
	tokens:  [dynamic]Token,
	tok_map: bytemap.Map(Token_Type),
	consumed:bit_array.Bit_Array,
	curr:    u32,
	q_count: u32,
}

Func_Group :: enum {
	None,
	Windowed,
	Aggregate,
	Scalar,
}

make_parser :: proc() -> Parser {
	return Parser {
		lf_vec = make([dynamic]u32),
		tokens = make([dynamic]Token),
		tok_map = bytemap.make_map(Token_Type, 256, {.No_Case}),
		consumed = bit_array.create(8),      /* why arg here? */
	}
}

destroy_parser :: proc(p: ^Parser) {
	delete(p.lf_vec)
	delete(p.tokens)
	bytemap.destroy(&p.tok_map)
}

parse_get_pos :: proc(p: ^Parser, idx: u32) -> (line, off: u32) {
	line = 1
	for lf in p.lf_vec {
		if lf > idx {
			break
		}
		off = lf
		line += 1
	}
	off = idx - off
	return
}

token_get_pos :: proc(p: ^Parser, tok: ^Token) -> (line, off: u32) {
	return parse_get_pos(p, tok.begin)
}

token_to_string :: proc(p: ^Parser, tok: ^Token) -> string {
	if tok == nil {
		return ""
	}
	return p.text[tok.begin:tok.begin+tok.len]
}

parse_error :: proc(p: ^Parser, msg: string) -> Result {
	error_tok := p.tokens[p.curr]
	error_str := p.text[error_tok.begin:error_tok.begin+error_tok.len]
	line, off := parse_get_pos(p, p.tokens[p.curr].begin)

	fmt.eprintf("%s at `%s': (line: %d, pos: %d)\n", msg, error_str, line, off)
	return .Error
}

parse_parse :: proc(sql: ^Streamql, query_str: string) -> Result {
	p := &sql.parser
	p.text = query_str
	p.q_count = 0

	lex_lex(p) or_return
	p.curr = 0

	if _get_next_token(p) {
		return .Ok
	}

	_parse_enter(sql) or_return

	p.curr = 0

	for !_get_next_token(p) {
		if res, ok := bit_array.get(&p.consumed, p.curr); !res || !ok {
			parse_error(p, "token not consumed")
			return .Error
		}
	}

	return .Ok
}

@(private="file")
_get_prev_token_from_here :: proc(p: ^Parser, here: ^u32) -> bool {
	i := here^ - 1
	if i <= 0 {
		return true
	}
	for ; p.tokens[i].type != .Query_Begin && p.tokens[i].type == .Query_Comment; i -= 1 {}
	here^ = i
	return p.tokens[i].type == .Query_Begin
}

@(private="file")
_peek_prev_token :: proc(p: ^Parser, i: u32) -> u32 {
	i := i  // shut -vet up
	i -= 1
	for ; p.tokens[i].type != .Query_Begin && p.tokens[i].type == .Query_Comment; i -= 1 {}
	return i
}

@(private="file")
_get_next_token :: proc {_get_next_token_from_here, _get_next_token_from_curr}

@(private="file")
_get_next_token_from_here :: proc(p: ^Parser, here: ^u32) -> bool {
	i := here^ + 1
	if i >= u32(len(p.tokens)) {
		return true
	}
	for ; p.tokens[i].type != .Query_End && p.tokens[i].type == .Query_Comment; i += 1 {}
	here^ = i
	return p.tokens[i].type == .Query_End
}

@(private="file")
_get_next_token_from_curr :: proc(p: ^Parser) -> bool {
	i := p.curr + 1
	if i >= u32(len(p.tokens)) {
		return true
	}
	for ; p.tokens[i].type != .Query_End && p.tokens[i].type == .Query_Comment; i += 1 {}
	p.curr = i
	return p.tokens[i].type == .Query_End
}

@(private="file")
_peek_next_token :: proc(p: ^Parser, i: u32) -> u32 {
	i := i  // shut vet up
	i += 1
	for ; p.tokens[i].type != .Query_End && p.tokens[i].type == .Query_Comment; i += 1 {}
	return i
}

@(private="file")
_get_next_token_or_die :: proc(p: ^Parser) -> Result {
	if _get_next_token(p) {
		return parse_error(p, "unexpected EOF")
	}
	return .Ok
}

@(private="file")
_get_func_group :: proc(type: Token_Type) -> Func_Group {
	switch int(type) {
	case 300..399:
		return .Windowed
	case 400..499:
		return .Aggregate
	case 500..599:
		return .Scalar
	}
	return .None
}

@(private="file")
_send_column_or_const :: proc(sql: ^Streamql, begin: u32) -> Result {
	p := &sql.parser
	tok := &p.tokens[begin]
	bit_array.set(&p.consumed, begin)
	#partial switch tok.type {
	case .Sym_Asterisk:
		return parse_send_asterisk(sql, tok, nil)
	case .Query_Name:
		next_idx := _peek_next_token(p, begin)
		if p.tokens[next_idx].type == .Sym_Dot {
			bit_array.set(&p.consumed, next_idx)
			next_idx = _peek_next_token(p, next_idx)
			bit_array.set(&p.consumed, next_idx)
			field_name_tok := &p.tokens[next_idx]
			#partial switch field_name_tok.type {
			case .Query_Name:
				return parse_send_name(sql, field_name_tok, tok)
			case .Sym_Asterisk:
				return parse_send_asterisk(sql, field_name_tok, tok)
			case:
				return parse_error(p, "unexpected token")
			}
		}
		return parse_send_name(sql, tok, nil)
	case .Literal_Int:
		return parse_send_int(sql, tok)
	case .Literal_Float:
		return parse_send_float(sql, tok)
	case .Literal_String:
		return parse_send_string(sql, tok)
	case:
		return parse_error(p, "unexpected token")
	}
}

@(private="file")
_parse_case_stmt :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	return parse_error(p, "case statement parsing incomplete")
}

@(private="file")
_skip_between_stmt :: proc(p: ^Parser) -> Result {
	return parse_error(p, "between skipping incomplete")
}

@(private="file")
_parse_function :: proc(sql: ^Streamql, idx: ^u32, allow_star: bool) -> Result {
	p := &sql.parser
	loop: for idx^ < p.curr {
		_get_next_token(p, idx)
		expr_idx := idx^
		extra_level := _find_expression(p, idx, allow_star) or_return
		if extra_level > 1 {
			return parse_error(p, "unmatched '('")
		}
		_parse_expression_runner(sql,
		                         expr_idx,
		                         p.tokens[expr_idx].end_expr) or_return

		#partial switch p.tokens[idx^].type {
		case .Sym_Rparen:
			break loop
		case .Sym_Comma:
			bit_array.set(&p.consumed, idx^)
		case:
			return parse_error(p, "unexpected token")
		}
	}
	return .Ok
}

@(private="file")
_skip_expression_list :: proc(p: ^Parser, allow_star: bool) -> Result {
	loop: for {
		_get_next_token_or_die(p) or_return
		//expr_begin := p.curr
		_find_expression(p, &p.curr, allow_star) or_return

		#partial switch p.tokens[p.curr].type {
		case .Sym_Rparen:
			break loop
		case .Sym_Comma:
			continue
		case:
			return parse_error(p, "unexpected token")
		}
	}
	return .Ok
}

@(private="file")
_skip_subquery :: proc(p: ^Parser) -> Result {
	level := 1
	for level != 0 {
		_get_next_token_or_die(p) or_return
		#partial switch p.tokens[p.curr].type {
		case .Sym_Lparen:
			level += 1
		case .End_Of_Subquery:
			level -= 1
		case .Sym_Rparen:
			level -= 1
		case:
		}
	}

	if level > 0 {
		return parse_error(p, "failed to parse subquery")
	}

	p.tokens[p.curr].type = .End_Of_Subquery
	return .Ok
}

@(private="file")
_is_single_term :: proc(p: ^Parser, begin, end: u32) -> bool {
	term_found: bool
	has_table_name: bool

	//if p.tokens[begin].type == .Sym_Lparen {
	//	_get_next_token(p)
	//}

	for begin:= begin; begin != end; begin += 1 {
		#partial switch p.tokens[begin].type {
		case .Query_Comment:
			continue
		case .Sym_Rparen:
			return term_found
		case .Sym_Dot:
			if !term_found {
				return false
			}
			has_table_name = true
		case .Sym_Asterisk:
			fallthrough
		case .Query_Name:
			fallthrough
		case .Query_Variable:
			fallthrough
		case .Literal_Int:
			fallthrough
		case .Literal_Float:
			fallthrough
		case .Literal_String:
			if term_found && !has_table_name {
				return false
			}
			term_found = true
			has_table_name = false
		case:
			return false
		}
	}

	return true
}

@(private="file")
_token_in_current_expr :: proc(p: ^Parser, idx: u32, group: u16) -> bool {
	if p.tokens[idx].group != group {
		return false
	}
	res, ok := bit_array.get(&p.consumed, idx)
	return !res || !ok
}

@(private="file")
_parse_expression :: proc(sql: ^Streamql, begin, end: u32, group: u16) -> Result {
	p := &sql.parser
	group := group
	begin := begin
	end := end

	for p.tokens[begin].type == .Sym_Lparen &&
	     p.tokens[_peek_prev_token(p, end)].type == .Sym_Rparen {
		_get_next_token(p, &begin)
		_get_prev_token_from_here(p, &end)
	}

	if p.tokens[begin].type == .Sym_Lparen &&
	    p.tokens[_peek_next_token(p, begin)].type == .Select {
		parse_enter_subquery_const(sql)

		curr_bak := p.curr
		p.curr = begin

		_get_next_token(p)

		ret := _parse_select_stmt(sql)

		p.curr = curr_bak
		parse_leave_subquery_const(sql)
		return ret
	}

	/* Leaf node?? */
	if _is_single_term(p, begin, end) {
		return _send_column_or_const(sql, begin)
	}

	/* Lowest precedence first */
	for i := begin; i < end; i += 1 {
		if !_token_in_current_expr(p, i, group) {
			continue
		}
		#partial switch p.tokens[i].type {
		case .Sym_Plus:
			fallthrough
		case .Sym_Minus:
			fallthrough
		case .Sym_Bit_And:
			fallthrough
		case .Sym_Bit_Or:
			fallthrough
		case .Sym_Bit_Xor:
			bit_array.set(&p.consumed, i)
			parse_enter_function(sql, &p.tokens[i])
			_parse_expression_runner(sql, begin, i) or_return
			_parse_expression_runner(sql, i + 1, end) or_return
			parse_leave_function(sql, &p.tokens[i])
		case:
		}
	}

	/* multiplication derivatives */
	for i := begin; i < end; i += 1 {
		if !_token_in_current_expr(p, i, group) {
			continue
		}
		#partial switch p.tokens[i].type {
		case .Sym_Multiply:
			fallthrough
		case .Sym_Divide:
			fallthrough
		case .Sym_Modulus:
			bit_array.set(&p.consumed, i)
			parse_enter_function(sql, &p.tokens[i])
			_parse_expression_runner(sql, begin, i) or_return
			_parse_expression_runner(sql, i + 1, end) or_return
			parse_leave_function(sql, &p.tokens[i])
		case:
		}
	}

	/* unary expressions */
	for i := begin; i < end; i += 1 {
		if !_token_in_current_expr(p, i, group) {
			continue
		}
		#partial switch p.tokens[i].type {
		case .Sym_Plus_Unary:
			fallthrough
		case .Sym_Minus_Unary:
			fallthrough
		case .Sym_Bit_Not_Unary:
			bit_array.set(&p.consumed, i)
			parse_enter_function(sql, &p.tokens[i])
			_parse_expression_runner(sql, i + 1, end) or_return
			parse_leave_function(sql, &p.tokens[i])
		case:
		}
	}

	/* case expressions */
	for i := begin; i < end; i += 1 {
		if !_token_in_current_expr(p, i, group) {
			continue
		}
		if p.tokens[i].type == .Case {
			bit_array.set(&p.consumed, i)
			_parse_case_stmt(sql) or_return
		}
	}

	/* functions */
	for i := begin; i != end; i += 1 {
		if !_token_in_current_expr(p, i, group) {
			continue
		}
		if fn_group := _get_func_group(p.tokens[i].type); fn_group != .None {
			bit_array.set(&p.consumed, i)
			parse_enter_function(sql, &p.tokens[i])
			_get_next_token(p, &i)
			_parse_function(sql, &i, p.tokens[i].type == .Count) or_return
			parse_leave_function(sql, &p.tokens[i])
		}
	}

	return .Ok
}

@(private="file")
_parse_expression_runner :: proc(sql: ^Streamql, begin, end: u32) -> Result {
	p := &sql.parser
	min_group: u16 = bits.U16_MAX
	max_group: u16 = 0

	for i := begin; i < end; i += 1 {
		if p.tokens[i].group > max_group {
			max_group = p.tokens[i].group
		}
		if p.tokens[i].group < min_group {
			min_group = p.tokens[i].group
		}
	}

	for i := min_group; i <= max_group; i += 1 {
		_parse_expression(sql, begin, end, i) or_return
	}

	return .Ok
}

@(private="file")
_Expr_State :: enum {
	None,
	Expect_Op_Or_End,
	Expect_Val,
	In_Function,
	In_Case,
}

@(private="file")
_find_expression :: proc(p: ^Parser, idx: ^u32, allow_star: bool) -> (level: int, ret: Result) {
	/* This variable represents the lowest legal level
	 * for exiting the expression.  It is required for
	 * expressions like:
	 *
	 * SELECT 1
	 * WHERE (1+1)=2 AND 3=3
	 *       ^~~ We don't (currently) know if this '('
	 *           belongs to the expression or not. Here
	 *           it does, but...
	 *
	 * SELECT 1
	 * WHERE (1+1 = 2 OR 3=3) AND 4=4
	 *       ^~~ ...here, it does not.
	 */

	lowest_exit_level : int
	first_token_vec := make([dynamic]u32)
	defer delete(first_token_vec)

	loop: for p.tokens[idx^].type != .Query_End  {
		append(&first_token_vec, idx^)
		#partial switch p.tokens[idx^].type {
		case .Sym_Lparen:
			next := _peek_next_token(p, idx^)
			/* if this is a subquery const, we want to
			 * break out of this loop all together
			 */
			if p.tokens[next].type == .Select {
				break loop
			}
			level += 1
			lowest_exit_level += 1
		case .Not:
		case:
			break loop
		}
		_get_next_token(p, idx)
	}

	state := _Expr_State.Expect_Val
	in_expr := true
	begin := idx^

	fn_token : ^Token

	for p.tokens[idx^].type != .Query_End {
		next_state := _Expr_State.None
		switch state {
		case .Expect_Val:
			#partial switch p.tokens[idx^].type {
			case .Not: /* If we were sent here from _parse_boolean... */
				next_state = .Expect_Val
				pop(&first_token_vec)
			case .Sym_Multiply:
				if !allow_star {
					return 0, parse_error(p, "unexpected token")
				}
				p.tokens[idx^].type = .Sym_Asterisk
				next_state = .Expect_Op_Or_End
			case .Sym_Plus:
				p.tokens[idx^].type = .Sym_Plus_Unary
				next_state = .Expect_Val
			case .Sym_Minus:
				p.tokens[idx^].type = .Sym_Minus_Unary
				next_state = .Expect_Val
			case .Sym_Bit_Not_Unary:
				next_state = .Expect_Val
			case .Case:
				next_state = .In_Case
			case .Query_Name .. .Literal_String:
				next_state = .Expect_Op_Or_End
			case .Sym_Lparen:
				next := _peek_next_token(p, idx^)
				/* if this is a Subquery Const, we need to skip over
				 * all the tokens belonging to the subquery...
				 */
				if p.tokens[next].type == .Select {
					next_state = .Expect_Op_Or_End
					_skip_subquery(p) or_return
				} else {
					level += 1
					next_state = .Expect_Val
				}
			case:
				fn_group := _get_func_group(p.tokens[idx^].type)
				if fn_group == .None {
					in_expr = false
				}
				next := _peek_next_token(p, idx^)
				if p.tokens[next].type != .Sym_Lparen {
					/* If next token isn't '(' then treat as name */
					p.tokens[idx^].type = .Query_Name
					next_state = .Expect_Op_Or_End
				} else {
					fn_token = &p.tokens[idx^]
					next_state = .In_Function
					//level += 1
				}
			}
		case .Expect_Op_Or_End:
			#partial switch p.tokens[idx^].type {
			case .Sym_Rparen:
				if level == 0 {
					in_expr = false
				} else {
					if level-1 < lowest_exit_level {
						lowest_exit_level -= 1
						pop(&first_token_vec)
					}
					level -= 1
					next_state = .Expect_Op_Or_End
				}
			case .Sym_Dot:
				fallthrough
			case .Sym_Plus:
				fallthrough
			case .Sym_Minus:
				fallthrough
			case .Sym_Multiply:
				fallthrough
			case .Sym_Divide:
				fallthrough
			case .Sym_Modulus:
				fallthrough
			case .Sym_Bit_Not_Unary:
				fallthrough
			case .Sym_Bit_Or:
				fallthrough
			case .Sym_Bit_And:
				fallthrough
			case .Sym_Bit_Xor:
				next_state = .Expect_Val
			case:
				next_state = state
				in_expr = false
			}
		case .In_Function:
			_skip_expression_list(p, fn_token.type == .Count)
			next_state = .Expect_Op_Or_End
		case .In_Case:
			return 0, parse_error(p, "case expressions incomplete")
		case .None:
			in_expr = false
		}

		if !in_expr {
			break
		}

		state = next_state

		_get_next_token(p, idx)
	}

	begin = pop(&first_token_vec)
	p.tokens[begin].end_expr = idx^

	if state != .Expect_Op_Or_End || level != lowest_exit_level {
		return 0, parse_error(p, "unexpected end of expression")
	}

	return level, .Ok
}

/* Should not discover any syntax errors at this point */
@(private="file")
_is_single_boolean_expression :: proc (p: ^Parser, begin, end: u32) -> bool {
	begin := begin

	/* Find beginning of left side expression */
	for ; begin < end && p.tokens[begin].end_expr == 0; begin += 1 { }

	begin = p.tokens[begin].end_expr - 1 /* Back the fuck up */

	_get_next_token(p, &begin)

	if p.tokens[begin].type == .Not {
		_get_next_token(p, &begin)
	}

	/* We should now be ON an comparison operator */
	#partial switch p.tokens[begin].type {
	case .Sym_Eq:
		fallthrough
	case .Sym_Ne:
		fallthrough
	case .Sym_Gt:
		fallthrough
	case .Sym_Ge:
		fallthrough
	case .Sym_Lt:
		fallthrough
	case .Sym_Le:
		fallthrough
	case .Like:
		_get_next_token(p, &begin)
		if p.tokens[begin].end_expr == 0 {
			return false
		}
	case .In:
		fallthrough
	case .Between:
		/* TODO */
		return false
	case:
		return false
	}

	return p.tokens[begin].end_expr >= end
}

@(private="file")
_parse_send_predicate :: proc(sql: ^Streamql, begin: u32) -> Result {
	p := &sql.parser
	begin := begin

	not_count := 0   /* Number of NOTs before predicate */

	for p.tokens[begin].type == .Not {
		not_count += 1
		bit_array.set(&p.consumed, begin)
		_get_next_token(p, &begin)
	}

	/* Find beginning of left side expression */
	for ; p.tokens[begin].end_expr == 0; begin += 1 {}

	left := begin
	begin = p.tokens[begin].end_expr - 1; /* Back the fuck up */

	_get_next_token(p, &begin)

	/* only for NOT IN/LIKE/BETWEEN */
	is_not := p.tokens[begin].type == .Not
	if is_not {
		bit_array.set(&p.consumed, begin)
		_get_next_token(p, &begin)
	}

	oper := begin

	/* only for leading NOTs */
	is_not_predicate := not_count % 2 == 1

	/* We should now be ON an comparison operator */
	#partial switch p.tokens[oper].type {
	case .Sym_Eq:
		fallthrough
	case .Sym_Ne:
		fallthrough
	case .Sym_Gt:
		fallthrough
	case .Sym_Ge:
		fallthrough
	case .Sym_Lt:
		fallthrough
	case .Sym_Le:
		if is_not {
			return parse_error(p, "unexpected NOT")
		}
		fallthrough
	case .Like:
		bit_array.set(&p.consumed, oper)
		parse_enter_predicate(sql, &p.tokens[oper], is_not_predicate, is_not)
		_get_next_token(p, &begin); /* begin now pointing at right side expr */
		left_tok := p.tokens[left]
		begin_tok := p.tokens[begin]
		_parse_expression_runner(sql, left, left_tok.end_expr) or_return
		_parse_expression_runner(sql, begin, begin_tok.end_expr) or_return
		parse_leave_predicate(sql, &p.tokens[oper], is_not_predicate, is_not)
	case .In:
		fallthrough
	case .Between:
		/* TODO */
		bit_array.set(&p.consumed, p.curr)
		return parse_error(p, "in/between current incomplete")
	case:
		return parse_error(p, "unexpected token in predicate")
	}

	return .Ok
}

@(private="file")
_parse_boolean_expression :: proc(sql: ^Streamql, begin, end: u32, group: u16) -> Result {
	p := &sql.parser
	begin := begin
	end := end

	for p.tokens[begin].type == .Sym_Lparen &&
	    p.tokens[_peek_prev_token(p, end)].type == .Sym_Rparen {
		_get_next_token(p, &begin)
		_get_prev_token_from_here(p, &end)
	}

	/* Shortcut for leaf node */
	if _is_single_boolean_expression(p, begin, end) {
		return _parse_send_predicate(sql, begin)
	}

	/* First, split on OR */
	for i := begin; i != end; i += 1 {
		if !_token_in_current_expr(p, i, group) {
			continue
		}
		#partial switch p.tokens[i].type {
		case .Or:
			bit_array.set(&p.consumed, i)
			parse_enter_or(sql)
			_parse_boolean_expression_runner(sql, begin, i) or_return
			_parse_boolean_expression_runner(sql, i + 1, end) or_return
			parse_leave_or(sql)
		case:
		}
	}

	/* Split on AND */
	for i := begin; i != end; i += 1 {
		if !_token_in_current_expr(p, i, group) {
			continue
		}
		#partial switch p.tokens[i].type {
		case .And:
			bit_array.set(&p.consumed, i)
			parse_enter_and(sql)
			_parse_boolean_expression_runner(sql, begin, i) or_return
			_parse_boolean_expression_runner(sql, i + 1, end) or_return
			parse_leave_and(sql)
		case:
		}
	}

	/* Split on NOT */
	for i := begin; i != end; i += 1 {
		if !_token_in_current_expr(p, i, group) {
			continue
		}
		#partial switch p.tokens[i].type {
		case .Not:
			bit_array.set(&p.consumed, i)
			parse_enter_not(sql)
			_parse_boolean_expression_runner(sql, i + 1, end) or_return
			parse_leave_not(sql)
		case:
		}
	}

	return .Ok
}
@(private="file")
_parse_boolean_expression_runner :: proc(sql: ^Streamql, begin, end: u32) -> Result {
	p := &sql.parser
	min_group: u16 = bits.U16_MAX
	max_group: u16 = 0

	for i := begin; i < end; i += 1 {
		if p.tokens[i].group > max_group {
			max_group = p.tokens[i].group
		}
		if p.tokens[i].group < min_group {
			min_group = p.tokens[i].group
		}
	}

	for i := min_group; i <= max_group; i += 1 {
		_parse_boolean_expression(sql, begin, end, i) or_return
	}

	return .Ok
}

/* TODO: convert parse_expression to have a skip option, because
 *       this becomes quite difficult when I don't know where an
 *       expression ends...
 */
@(private="file")
_bool_state :: enum {
	Expect_Logic_Or_End,
	Expect_Expression,
	Expect_Expression_Or_Not,
	Expect_Comparison,
}

@(private="file")
_find_boolean_expression :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	state := _bool_state.Expect_Expression_Or_Not

	begin := p.curr

	level : int
	left_side := true

	/* This is very similar to the first pass of _find_expression
	 * except, there is no real "interpretation" here. The only
	 * purpose of this pass is to split the logic based on the
	 * following precedecnce:  (), AND, OR
	 */
	loop: for p.tokens[p.curr].type != .Query_End {
		switch state {
		case .Expect_Expression_Or_Not:
			if p.tokens[p.curr].type == .Not {
				_get_next_token(p)
				continue
			}
			fallthrough
		case .Expect_Expression:
			curr_token := p.curr
			extra_level := _find_expression(p, &p.curr, false) or_return
			for ; extra_level != 0; curr_token -= 1 {
				level += 1
				extra_level -= 1
			}
			if left_side {
				state = .Expect_Comparison
			} else {
				state = .Expect_Logic_Or_End
			}
			left_side = !left_side
		case .Expect_Comparison:
			#partial switch p.tokens[p.curr].type {
			case .Not:
				_get_next_token_or_die(p) or_return
			case .Sym_Eq:
				fallthrough
			case .Sym_Ne:
				fallthrough
			case .Sym_Lt:
				fallthrough
			case .Sym_Le:
				fallthrough
			case .Sym_Gt:
				fallthrough
			case .Sym_Ge:
				fallthrough
			case .Like:
				_get_next_token_or_die(p) or_return
				state = .Expect_Expression
			case .Between:
				_skip_between_stmt(p) or_return
				state = .Expect_Logic_Or_End
			case .In:
				_skip_expression_list(p, false) or_return
				state = .Expect_Logic_Or_End
			case:
				return parse_error(p, "Unexpected token")
			}
		case .Expect_Logic_Or_End:
			#partial switch p.tokens[p.curr].type {
			case .Sym_Rparen:
				level -= 1
			case .And:
				fallthrough
			case .Or:
				state = .Expect_Expression_Or_Not
			case:
				//return parse_error(p, "unexpected token")
				break loop
			}
			_get_next_token(p)
		}
	}

	if state != .Expect_Logic_Or_End || level != 0 {
		return parse_error(p, "unexpected end of boolean expression")
	}

	return _parse_boolean_expression_runner(sql, begin, p.curr)
}

@(private="file")
_parse_execute_stmt :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	return parse_error(p, "proc execution incomplete")
}

@(private="file")
_parse_into_stmt :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	_get_next_token_or_die(p) or_return
	parse_send_into_name(sql, &p.tokens[p.curr]) or_return
	if _get_next_token(p) {
		return .Ok
	}

	#partial switch p.tokens[p.curr].type {
	case .From:
		return _parse_from_stmt(sql)
	case .Where:
		return _parse_where_stmt(sql)
	case .Group:
		return _parse_groupby_stmt(sql)
	case .Having:
		return _parse_having_stmt(sql)
	case .End_Of_Subquery:
		return .Ok
	case:
		return _parse_enter(sql)
	}
}

@(private="file")
_parse_subquery_source :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	parse_enter_subquery_source(sql)

	at_select := p.curr

	/* use this to mark the end of the subquery */
	_skip_subquery(p) or_return
	after_subquery := p.curr

	/* prepare to parse subquery */
	p.curr = at_select

	ret := _parse_select_stmt(sql)

	/* ready to begin parsing higher level query now */
	p.curr = after_subquery

	parse_leave_subquery_source(sql)
	return ret
}

@(private="file")
_parse_source_item :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	_get_next_token_or_die(p) or_return

	name_chain : [4]^Token
	i : int

	loop: for ;; i += 1 {
		/* Parse source name or subquery source */
		#partial switch p.tokens[p.curr].type {
		case .Query_Variable:
			fallthrough
		case .Query_Name:
			bit_array.set(&p.consumed, p.curr)
			name_chain[i] = &p.tokens[p.curr]
		case .Sym_Lparen:
			if i > 0 {
				return parse_error(p, "unexpected chaining")
			}
			_get_next_token_or_die(p) or_return
			if p.tokens[p.curr].type != .Select {
				return parse_error(p, "expected subquery")
			}
			//bit_array.set(&p.consumed, p.curr)
			_parse_subquery_source(sql)
			i = -1
			break loop
		case:
			return parse_error(p, "unexpected token")
		}

		next := _peek_next_token(p, p.curr)
		if i == 3 || p.tokens[next].type != .Sym_Dot {
			break loop
		}
		_get_next_token(p)
		bit_array.set(&p.consumed, p.curr)
		_get_next_token_or_die(p) or_return
	}

	/* Not a subquery... */
	if i >= 0 {
		parse_send_table_source(sql, name_chain[:i + 1])
	}

	if _get_next_token(p) {
		return .Ok
	}

	/* check for alias */
	if p.tokens[p.curr].type == .As {
		bit_array.set(&p.consumed, p.curr)
		_get_next_token_or_die(p) or_return
	}

	if p.tokens[p.curr].type == .Query_Name ||
	    p.tokens[p.curr].type == .Query_Variable {
		bit_array.set(&p.consumed, p.curr)
		parse_send_source_alias(sql, &p.tokens[p.curr])
		_get_next_token(p)
	}

	return .Ok
}

@(private="file")
_parse_from_stmt :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	//parse_enter_from(sql)
	in_source_list := true

	bit_array.set(&p.consumed, p.curr)

	_parse_source_item(sql) or_return

	/* Condition currently redundant */
	for in_source_list {
		join_type_idx := p.curr
		expect_on := false

		/* check for join */
		#partial switch p.tokens[join_type_idx].type {
		case .Inner:
			fallthrough
		case .Left:
			fallthrough
		case .Right:
			fallthrough
		case .Full:
			expect_on = true
			fallthrough
		case .Cross:
			bit_array.set(&p.consumed, p.curr)
			_get_next_token_or_die(p) or_return
			bit_array.set(&p.consumed, p.curr)
			if p.tokens[p.curr].type != .Join {
				return parse_error(p, "expected JOIN")
			}
		case .Join:
			bit_array.set(&p.consumed, p.curr)
			p.tokens[join_type_idx].type = .Inner
			expect_on = true
		case .Sym_Comma: /* cross join */
			bit_array.set(&p.consumed, p.curr)
			p.tokens[join_type_idx].type = .Cross
		case:
			in_source_list = false
		}

		if !in_source_list {
			break
		}

		parse_send_join_type(sql, &p.tokens[join_type_idx]) or_return
		_parse_source_item(sql) or_return

		if expect_on {
			if p.tokens[p.curr].type != .On {
				return parse_error(p, "expected ON")
			}
			bit_array.set(&p.consumed, p.curr)
			_get_next_token_or_die(p) or_return
			parse_enter_join_logic(sql)
			_find_boolean_expression(sql) or_return
			parse_leave_join_logic(sql)
		}
	}

	//parse_leave_from(sql)

	#partial switch p.tokens[p.curr].type {
	case .Where:
		return _parse_where_stmt(sql)
	case .Group:
		return _parse_groupby_stmt(sql)
	case .Having:
		return _parse_having_stmt(sql)
	case .End_Of_Subquery:
		return .Ok
	case:
		return _parse_enter(sql)
	}
}

@(private="file")
_parse_where_stmt :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	bit_array.set(&p.consumed, p.curr)
	_get_next_token_or_die(p) or_return
	parse_enter_where(sql)
	_find_boolean_expression(sql) or_return
	parse_leave_where(sql)

	#partial switch p.tokens[p.curr].type {
	case .Group:
		return _parse_groupby_stmt(sql)
	case .Having:
		return _parse_having_stmt(sql)
	case .End_Of_Subquery:
		return .Ok
	case:
		return _parse_enter(sql)
	}
}

@(private="file")
_parse_groupby_stmt :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	return parse_error(p, "group by incomplete")
}

@(private="file")
_parse_having_stmt :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	return parse_error(p, "having incomplete")
}

@(private="file")
_parse_select_list :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	for {
		expr_begin := p.curr
		extra_level := _find_expression(p, &p.curr, true) or_return
		if extra_level > 0 {
			return parse_error(p, "unmatched '('")
		}
		_parse_expression_runner(sql,
		                         expr_begin,
		                         p.tokens[expr_begin].end_expr) or_return

		if p.tokens[p.curr].type == .As {
			bit_array.set(&p.consumed, p.curr)
			_get_next_token_or_die(p) or_return
		}

		if p.tokens[p.curr].type == .Query_Name {
			bit_array.set(&p.consumed, p.curr)
			parse_send_column_alias(sql, &p.tokens[p.curr])
			if _get_next_token(p) {
				return .Ok
			}
		}

		#partial switch p.tokens[p.curr].type {
		case .Sym_Comma:
			bit_array.set(&p.consumed, p.curr)
			_get_next_token_or_die(p) or_return
		case .Into:
			return _parse_into_stmt(sql)
		case .From:
			return _parse_from_stmt(sql)
		case .Where:
			return _parse_where_stmt(sql)
		case .Group:
			return _parse_groupby_stmt(sql)
		case .Having:
			return _parse_having_stmt(sql)
		case .End_Of_Subquery:
			return .Ok
		case:
			return _parse_enter(sql)
		}
	}
	return .Ok
}

@(private="file")
_parse_select_stmt :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	parse_send_select_stmt(sql)
	bit_array.set(&p.consumed, p.curr)
	_get_next_token_or_die(p) or_return

	all_or_distinct_allowed := true
	top_allowed := true

	for {
		#partial switch p.tokens[p.curr].type {
		case .All:
			if !all_or_distinct_allowed {
				return parse_error(p, "unexpected token")
			}
			/* This is really a no-op anyway... */
			parse_send_all(sql)
			bit_array.set(&p.consumed, p.curr)
			all_or_distinct_allowed = false
		case .Distinct:
			if !all_or_distinct_allowed {
				return parse_error(p, "unexpected token")
			}
			parse_send_distinct(sql)
			bit_array.set(&p.consumed, p.curr)
			all_or_distinct_allowed = false
		case .Top:
			if !top_allowed {
				return parse_error(p, "unexpected token")
			}
			bit_array.set(&p.consumed, p.curr)

			_get_next_token_or_die(p) or_return
			parse_enter_top_expr(sql)
			expr_begin := p.curr

			extra_level := _find_expression(p, &p.curr, false) or_return
			if extra_level > 0 {
				return parse_error(p, "unmatched '('")
			}
			_parse_expression_runner(sql,
			                         expr_begin,
			                         p.tokens[expr_begin].end_expr) or_return
			parse_leave_top_expr(sql)
			all_or_distinct_allowed = false
			top_allowed = false
		case:
			return _parse_select_list(sql)
		}
	}
	return .Ok
}

@(private="file")
_parse_delete_stmt :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	return parse_error(p, "dead end")
}

@(private="file")
_parse_update_stmt :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	return parse_error(p, "dead end")
}

@(private="file")
_parse_insert_stmt :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	return parse_error(p, "not implemented")
}

@(private="file")
_parse_alter_stmt :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	return parse_error(p, "not implemented")
}

@(private="file")
_parse_create_stmt :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	return parse_error(p, "not implemented")
}

@(private="file")
_parse_drop_stmt :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	return parse_error(p, "not implemented")
}

@(private="file")
_parse_truncate_stmt :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	return parse_error(p, "not implemented")
}

@(private="file")
_parse_break_stmt :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	return parse_error(p, "not implemented")
}

@(private="file")
_parse_continue_stmt :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	return parse_error(p, "not implemented")
}

@(private="file")
_parse_goto_stmt :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	return parse_error(p, "not implemented")
}

@(private="file")
_parse_if_stmt :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	return parse_error(p, "dead end")
}

@(private="file")
_parse_return_stmt :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	return parse_error(p, "not implemented")
}

@(private="file")
_parse_waitfor_stmt :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	return parse_error(p, "not implemented")
}

@(private="file")
_parse_while_stmt :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	return parse_error(p, "not implemented")
}

@(private="file")
_parse_print_stmt :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	return parse_error(p, "not implemented")
}

@(private="file")
_parse_raiserror_stmt :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	return parse_error(p, "not implemented")
}

@(private="file")
_parse_enter :: proc(sql: ^Streamql) -> Result {
	p := &sql.parser
	if p.tokens[p.curr].type == .Query_End {
		return .Ok
	}

	p.q_count += 1

	ret : Result

	curr_tok := p.tokens[p.curr]
	preview_end := u32(len(p.text)) - curr_tok.begin
	if preview_end - curr_tok.begin > PREVIEW_MAX {
		preview_end = curr_tok.begin + PREVIEW_MAX
	}
	
	/* Clone so we can edit */
	allocated: bool
	query_text := strings.clone(p.text[curr_tok.begin:preview_end])
	query_text, allocated = strings.replace_all(query_text, "\n", " ")
	query_text, allocated = strings.replace_all(query_text, "\r", " ")

	parse_enter_sql(sql, query_text)

	#partial switch p.tokens[p.curr].type {
	case .Query_Name:
		ret = _parse_execute_stmt(sql)
	case .Select:
		ret = _parse_select_stmt(sql)
	case .Delete:
		ret = _parse_delete_stmt(sql)
	case .Update:
		ret = _parse_update_stmt(sql)
	case .Insert:
		ret = _parse_insert_stmt(sql)
	case .Alter:
		ret = _parse_alter_stmt(sql)
	case .Create:
		ret = _parse_create_stmt(sql)
	case .Drop:
		ret = _parse_drop_stmt(sql)
	case .Truncate:
		ret = _parse_truncate_stmt(sql)
	case .Break:
		ret = _parse_break_stmt(sql)
	case .Continue:
		ret = _parse_continue_stmt(sql)
	case .Goto:
		ret = _parse_goto_stmt(sql)
	case .If:
		ret = _parse_if_stmt(sql)
	case .Return:
		ret = _parse_return_stmt(sql)
	case .Waitfor:
		ret = _parse_waitfor_stmt(sql)
	case .While:
		ret = _parse_while_stmt(sql)
	case .Print:
		ret = _parse_print_stmt(sql)
	case .Raiserror:
		ret = _parse_raiserror_stmt(sql)
	case:
		p.q_count -= 1
		ret = parse_error(p, "unexpected token")
	}

	parse_leave_sql(sql)

	return ret
}

/* Let's fuck this shit up */
@(test)
parse_error_check :: proc (t: ^testing.T) {
	sql : Streamql
	construct(&sql, {.Parse_Only})

	ret: Result

	ret = parse_parse(&sql, "select")
	testing.expect_value(t, ret, Result.Error)

	ret = parse_parse(&sql, "select 1 = 2")
	testing.expect_value(t, ret, Result.Error)

	ret = parse_parse(&sql, "select 1 +* 2")
	testing.expect_value(t, ret, Result.Error)

	ret = parse_parse(&sql, "select a,b 34")
	testing.expect_value(t, ret, Result.Error)

	ret = parse_parse(&sql, "select a a a")
	testing.expect_value(t, ret, Result.Error)

	ret = parse_parse(&sql, "select 1(55+2)")
	testing.expect_value(t, ret, Result.Error)

	ret = parse_parse(&sql, "select from foo")
	testing.expect_value(t, ret, Result.Error)

	ret = parse_parse(&sql, "select 1 from a.b.c.d.e")
	testing.expect_value(t, ret, Result.Error)

	ret = parse_parse(&sql, "select 1 where 1=1 from foo")
	testing.expect_value(t, ret, Result.Error)

	ret = parse_parse(&sql, "select from foo where 1")
	testing.expect_value(t, ret, Result.Error)

	ret = parse_parse(&sql, "select from foo where 1 not = 1")
	testing.expect_value(t, ret, Result.Error)

	destroy(&sql)
}

@(test)
parse_check :: proc (t: ^testing.T) {
	sql : Streamql
	construct(&sql, {.Parse_Only})

	p := &sql.parser

	ret: Result

	ret = parse_parse(&sql, "select 1 a select /*my*/ 1")
	testing.expect_value(t, ret, Result.Ok)
	testing.expect_value(t, p.q_count, 2)

	ret = parse_parse(&sql, "select (select ( /*comments*/ select (1+2) from foo)) /*are*/ from (select 3) /*in*/ x")
	testing.expect_value(t, ret, Result.Ok)
	testing.expect_value(t, p.q_count, 1)

	ret = parse_parse(&sql, "select f /*really*/.* /*bad*/ from foo f join bar b on f. /*locations*/seq = b.seq where 1=2")
	testing.expect_value(t, ret, Result.Ok)
	testing.expect_value(t, p.q_count, 1)

	ret = parse_parse(&sql, `
	    select ((((1+2)*3/*what*/)/foo) | 1 ) + 2
	    from foo -- you
	    where ((/*think*/(((1=1) and 2=2) or 3=3 and len('foo') = 3 and 5!=6)))
	`)
	testing.expect_value(t, ret, Result.Ok)
	testing.expect_value(t, p.q_count, 1)

	destroy(&sql)
}
