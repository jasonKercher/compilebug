//+private
package streamql

import "bytemap"

import "core:fmt"
import "core:strings"
import "core:unicode"
import "core:testing"
import "core:container/bit_array"

Token :: struct {
	begin:    u32,
	len:      u32,
	end_expr: u32,
	group:    u16,
	type:     Token_Type,
}

Token_Type :: enum u16 {
	/* Not Keywords */
	Query_Begin,    /* All parsing begins here... */
	Query_End,      /*    ... and ends here       */
	Query_Comment,
	Query_Name = 20,
	Query_Variable,
	Literal_Int,
	Literal_Float,
	Literal_String,

	/* Special un-mapped tokens */
	Sym_Asterisk = 50,  /* was Sym_Multiply */
	Sym_Plus_Unary,     /* was Sym_Plus */
	Sym_Minus_Unary,    /* was Sym_Minus */
	End_Of_Subquery,    /* was Sym_Rparen */

	/* Keywords */
	Day = 100,
	Month,
	Sign,
	Str,
	Year,
	Text,
	Ntext,
	Add,
	All,
	Alter,
	And,
	Any,
	As,
	Begin,
	Between,
	Break,
	By,
	Case,
	Column,
	Continue,
	Create,
	Cross,
	Declare,
	Delete,
	Desc,
	Distinct,
	Distributed,
	Drop,
	Else,
	End,
	Execute,
	Exists,
	From,
	Full,
	Function,
	Goto,
	Group,
	Having,
	If,
	In,
	Inner,
	Insert,
	Into,
	Is,
	Join,
	Like,
	Not,
	Null,
	Of,
	Off,
	On,
	Open,
	Or,
	Order,
	Over,
	Percent,
	Print,
	Proc,
	Procedure,
	Raiserror,
	Replication,
	Return,
	Revert,
	Rollback,
	Save,
	Schema,
	Select,
	Set,
	Table,
	Then,
	To,
	Top,
	Tran,
	Transaction,
	Truncate,
	Union,
	Update,
	User,
	Values,
	When,
	Where,
	While,
	Bigint,
	Checksum,
	Days,
	Go,
	Hash,
	Hours,
	Int,
	Minutes,
	Range,
	Row,
	Rows,
	Seconds,
	Smallint,
	Static,
	Statusonly,
	Tinyint,
	Wait,
	Waitfor,
	Varchar,
	Nvarchar,

	/* Windowed Functions */
	Dense_Rank = 300,
	Rank,
	Row_Number,

	/* Aggregate Functions */
	Avg = 400,
	Checksum_Agg,
	Count,
	Max,
	Min,
	Stdev,
	Stdevp,
	String_Agg,
	Sum,

	/* Scalar Functions */
	Abs = 500,
	Ascii,
	Cast,
	Ceiling,
	Char,
	Charindex,
	Coalesce,
	Concat,
	Convert,
	Datalength,
	Dateadd,
	Datediff,
	Datename,
	Datepart,
	Floor,
	Getdate,
	Getutcdate,
	Isdate,
	Isnull,
	Isnumeric,
	Left,
	Len,
	Lower,
	Ltrim,
	Nchar,
	Nullif,
	Patindex,
	Rand,
	Replace,
	Right,
	Round,
	Rtrim,
	Space,
	Stuff,
	Substring,
	Try_Cast,
	Upper,
	User_Name,

	/* symbols */
	Sym_Pound = 600,
	Sym_Lparen,
	Sym_Rparen,
	Sym_Plus_Assign,
	Sym_Minus_Assign,
	Sym_Multiply_Assign,
	Sym_Divide_Assign,
	Sym_Modulus_Assign,
	Sym_Bit_Not_Assign,
	Sym_Bit_Or_Assign,
	Sym_Bit_And_Assign,
	Sym_Bit_Xor_Assign,
	Sym_Plus,
	Sym_Minus,
	Sym_Multiply,
	Sym_Divide,
	Sym_Modulus,
	Sym_Bit_Not_Unary,
	Sym_Bit_Or,
	Sym_Bit_And,
	Sym_Bit_Xor,
	Sym_Dot,
	Sym_Eq,
	Sym_Ne,
	Sym_Gt,
	Sym_Ge,
	Sym_Lt,
	Sym_Le,
	Sym_Comma,
	Sym_Semicolon,
	Sym_Block_Comment,
	Sym_Line_Comment,
}

lex_lex :: proc (p: ^Parser) -> Result {
	if len(p.tok_map.values) == 0 {
		_init_map(p)
	}

	resize(&p.tokens, 0)
	resize(&p.lf_vec, 0)
	bit_array.clear(&p.consumed)

	return _lex_tokenize(p)
}

lex_error :: proc(p: ^Parser, idx: u32, msg: string = "lex error") -> Result {
	line, off := parse_get_pos(p, idx)
	fmt.eprintf("%s (line: %d, pos: %d)\n", msg, line, off)
	return .Error
}

@(private="file")
_insert_into_map :: proc(p: ^Parser, key: string, type: Token_Type) {
	ret := bytemap.set(&p.tok_map, key, type)
	assert(!ret)
}

@(private="file")
_init_map :: proc(p: ^Parser) {
	_insert_into_map(p, "abs",          .Abs)
	_insert_into_map(p, "ascii",        .Ascii)
	_insert_into_map(p, "ceiling",      .Ceiling)
	_insert_into_map(p, "char",         .Char)
	_insert_into_map(p, "charindex",    .Charindex)
	_insert_into_map(p, "datalength",   .Datalength)
	_insert_into_map(p, "day",          .Day)
	_insert_into_map(p, "floor",        .Floor)
	_insert_into_map(p, "isdate",       .Isdate)
	_insert_into_map(p, "isnumeric",    .Isnumeric)
	_insert_into_map(p, "len",          .Len)
	_insert_into_map(p, "lower",        .Lower)
	_insert_into_map(p, "ltrim",        .Ltrim)
	_insert_into_map(p, "month",        .Month)
	_insert_into_map(p, "nchar",        .Nchar)
	_insert_into_map(p, "patindex",     .Patindex)
	_insert_into_map(p, "rand",         .Rand)
	_insert_into_map(p, "replace",      .Replace)
	_insert_into_map(p, "round",        .Round)
	_insert_into_map(p, "rtrim",        .Rtrim)
	_insert_into_map(p, "sign",         .Sign)
	_insert_into_map(p, "space",        .Space)
	_insert_into_map(p, "str",          .Str)
	_insert_into_map(p, "substring",    .Substring)
	_insert_into_map(p, "upper",        .Upper)
	_insert_into_map(p, "user_name",    .User_Name)
	_insert_into_map(p, "year",         .Year)
	_insert_into_map(p, "text",         .Text)
	_insert_into_map(p, "ntext",        .Ntext)
	_insert_into_map(p, "add",          .Add)
	_insert_into_map(p, "all",          .All)
	_insert_into_map(p, "alter",        .Alter)
	_insert_into_map(p, "and",          .And)
	_insert_into_map(p, "any",          .Any)
	_insert_into_map(p, "as",           .As)
	_insert_into_map(p, "begin",        .Begin)
	_insert_into_map(p, "between",      .Between)
	_insert_into_map(p, "break",        .Break)
	_insert_into_map(p, "by",           .By)
	_insert_into_map(p, "case",         .Case)
	_insert_into_map(p, "coalesce",     .Coalesce)
	_insert_into_map(p, "column",       .Column)
	_insert_into_map(p, "continue",     .Continue)
	_insert_into_map(p, "convert",      .Convert)
	_insert_into_map(p, "create",       .Create)
	_insert_into_map(p, "cross",        .Cross)
	_insert_into_map(p, "declare",      .Declare)
	_insert_into_map(p, "delete",       .Delete)
	_insert_into_map(p, "desc",         .Desc)
	_insert_into_map(p, "distinct",     .Distinct)
	_insert_into_map(p, "distributed",  .Distributed)
	_insert_into_map(p, "drop",         .Drop)
	_insert_into_map(p, "else",         .Else)
	_insert_into_map(p, "end",          .End)
	_insert_into_map(p, "execute",      .Execute)
	_insert_into_map(p, "exists",       .Exists)
	_insert_into_map(p, "from",         .From)
	_insert_into_map(p, "full",         .Full)
	_insert_into_map(p, "function",     .Function)
	_insert_into_map(p, "goto",         .Goto)
	_insert_into_map(p, "group",        .Group)
	_insert_into_map(p, "having",       .Having)
	_insert_into_map(p, "if",           .If)
	_insert_into_map(p, "in",           .In)
	_insert_into_map(p, "inner",        .Inner)
	_insert_into_map(p, "insert",       .Insert)
	_insert_into_map(p, "into",         .Into)
	_insert_into_map(p, "is",           .Is)
	_insert_into_map(p, "join",         .Join)
	_insert_into_map(p, "left",         .Left)
	_insert_into_map(p, "like",         .Like)
	_insert_into_map(p, "not",          .Not)
	_insert_into_map(p, "null",         .Null)
	_insert_into_map(p, "nullif",       .Nullif)
	_insert_into_map(p, "of",           .Of)
	_insert_into_map(p, "off",          .Off)
	_insert_into_map(p, "on",           .On)
	_insert_into_map(p, "open",         .Open)
	_insert_into_map(p, "or",           .Or)
	_insert_into_map(p, "order",        .Order)
	_insert_into_map(p, "over",         .Over)
	_insert_into_map(p, "percent",      .Percent)
	_insert_into_map(p, "print",        .Print)
	_insert_into_map(p, "proc",         .Proc)
	_insert_into_map(p, "procedure",    .Procedure)
	_insert_into_map(p, "raiserror",    .Raiserror)
	_insert_into_map(p, "replication",  .Replication)
	_insert_into_map(p, "return",       .Return)
	_insert_into_map(p, "revert",       .Revert)
	_insert_into_map(p, "right",        .Right)
	_insert_into_map(p, "rollback",     .Rollback)
	_insert_into_map(p, "save",         .Save)
	_insert_into_map(p, "schema",       .Schema)
	_insert_into_map(p, "select",       .Select)
	_insert_into_map(p, "set",          .Set)
	_insert_into_map(p, "table",        .Table)
	_insert_into_map(p, "then",         .Then)
	_insert_into_map(p, "to",           .To)
	_insert_into_map(p, "top",          .Top)
	_insert_into_map(p, "tran",         .Tran)
	_insert_into_map(p, "transaction",  .Transaction)
	_insert_into_map(p, "truncate",     .Truncate)
	_insert_into_map(p, "union",        .Union)
	_insert_into_map(p, "update",       .Update)
	_insert_into_map(p, "user",         .User)
	_insert_into_map(p, "values",       .Values)
	_insert_into_map(p, "when",         .When)
	_insert_into_map(p, "where",        .Where)
	_insert_into_map(p, "while",        .While)
	_insert_into_map(p, "avg",          .Avg)
	_insert_into_map(p, "bigint",       .Bigint)
	_insert_into_map(p, "cast",         .Cast)
	_insert_into_map(p, "try_cast",     .Try_Cast)
	_insert_into_map(p, "checksum",     .Checksum)
	_insert_into_map(p, "checksum_agg", .Checksum_Agg)
	_insert_into_map(p, "concat",       .Concat)
	_insert_into_map(p, "count",        .Count)
	_insert_into_map(p, "dateadd",      .Dateadd)
	_insert_into_map(p, "datediff",     .Datediff)
	_insert_into_map(p, "datename",     .Datename)
	_insert_into_map(p, "datepart",     .Datepart)
	_insert_into_map(p, "days",         .Days)
	_insert_into_map(p, "dense_rank",   .Dense_Rank)
	_insert_into_map(p, "getdate",      .Getdate)
	_insert_into_map(p, "getutcdate",   .Getutcdate)
	_insert_into_map(p, "go",           .Go)
	_insert_into_map(p, "hash",         .Hash)
	_insert_into_map(p, "hours",        .Hours)
	_insert_into_map(p, "int",          .Int)
	_insert_into_map(p, "max",          .Max)
	_insert_into_map(p, "min",          .Min)
	_insert_into_map(p, "minutes",      .Minutes)
	_insert_into_map(p, "range",        .Range)
	_insert_into_map(p, "rank",         .Rank)
	_insert_into_map(p, "row",          .Row)
	_insert_into_map(p, "row_number",   .Row_Number)
	_insert_into_map(p, "rows",         .Rows)
	_insert_into_map(p, "seconds",      .Seconds)
	_insert_into_map(p, "smallint",     .Smallint)
	_insert_into_map(p, "static",       .Static)
	_insert_into_map(p, "statusonly",   .Statusonly)
	_insert_into_map(p, "stdev",        .Stdev)
	_insert_into_map(p, "stdevp",       .Stdevp)
	_insert_into_map(p, "string_agg",   .String_Agg)
	_insert_into_map(p, "stuff",        .Stuff)
	_insert_into_map(p, "sum",          .Sum)
	_insert_into_map(p, "tinyint",      .Tinyint)
	_insert_into_map(p, "wait",         .Wait)
	_insert_into_map(p, "waitfor",      .Waitfor)
	_insert_into_map(p, "isnull",       .Isnull)
	_insert_into_map(p, "varchar",      .Varchar)
	_insert_into_map(p, "nvarchar",     .Nvarchar)

	_insert_into_map(p, "#",  .Sym_Pound)
	_insert_into_map(p, "(",  .Sym_Lparen)
	_insert_into_map(p, ")",  .Sym_Rparen)
	_insert_into_map(p, "+=", .Sym_Plus_Assign)
	_insert_into_map(p, "-=", .Sym_Minus_Assign)
	_insert_into_map(p, "*=", .Sym_Multiply_Assign)
	_insert_into_map(p, "/=", .Sym_Divide_Assign)
	_insert_into_map(p, "%=", .Sym_Modulus_Assign)
	_insert_into_map(p, "~=", .Sym_Bit_Not_Assign)
	_insert_into_map(p, "|=", .Sym_Bit_Or_Assign)
	_insert_into_map(p, "&=", .Sym_Bit_And_Assign)
	_insert_into_map(p, "^=", .Sym_Bit_Xor_Assign)
	_insert_into_map(p, "+",  .Sym_Plus)
	_insert_into_map(p, "-",  .Sym_Minus)
	_insert_into_map(p, "*",  .Sym_Multiply)
	_insert_into_map(p, "/",  .Sym_Divide)
	_insert_into_map(p, "%",  .Sym_Modulus)
	_insert_into_map(p, "~",  .Sym_Bit_Not_Unary)
	_insert_into_map(p, "|",  .Sym_Bit_Or)
	_insert_into_map(p, "&",  .Sym_Bit_And)
	_insert_into_map(p, "^",  .Sym_Bit_Xor)
	_insert_into_map(p, ".",  .Sym_Dot)
	_insert_into_map(p, "=",  .Sym_Eq)
	_insert_into_map(p, "!=", .Sym_Ne)
	_insert_into_map(p, "<>", .Sym_Ne)
	_insert_into_map(p, ">",  .Sym_Gt)
	_insert_into_map(p, ">=", .Sym_Ge)
	_insert_into_map(p, "<",  .Sym_Lt)
	_insert_into_map(p, "<=", .Sym_Le)
	_insert_into_map(p, ",",  .Sym_Comma)
	_insert_into_map(p, ";",  .Sym_Semicolon)
	_insert_into_map(p, "/*", .Sym_Block_Comment)
	_insert_into_map(p, "--", .Sym_Line_Comment)
}

@(private="file")
_skip_whitespace :: proc(p: ^Parser, idx: ^u32)
{
	for ; idx^ < u32(len(p.text)) && unicode.is_space(rune(p.text[idx^])); idx^ += 1 {
		if p.text[idx^] == '\n' {
			append(&p.lf_vec, idx^)
		}
	}
}

@(private="file")
_get_name :: proc(p: ^Parser, group: int, idx: ^u32) {
	begin := idx^
	for ; idx^ < u32(len(p.text)) && (p.text[idx^] == '_' ||
	      unicode.is_digit(rune(p.text[idx^])) ||
	      unicode.is_alpha(rune(p.text[idx^]))); idx^ += 1 {}

	type, ret := bytemap.get(&p.tok_map, p.text[begin:idx^])
	if !ret {
		type = .Query_Name
	}

	append(&p.tokens, Token {
		    type=type,
		    group=u16(group),
		    begin=begin,
		    len=idx^-begin })
}

@(private="file")
_get_string :: proc(p: ^Parser, group: int, idx: ^u32) -> Result {
	idx^ += 1
	real_begin := idx^
	for ; idx^ < u32(len(p.text)) && p.text[idx^] != '\''; idx^ += 1 {}

	if idx^ >= u32(len(p.text)) {
		return lex_error(p, idx^, "unmatched '\''")
	}

	idx^ += 1

	real_end := idx^ - 1
	if real_begin == idx^ {
		real_end = real_begin
	}

	append(&p.tokens, Token {
		    type = .Literal_String,
		    group=u16(group),
		    begin = real_begin,
		    len = real_end-real_begin })

	return .Ok
}

@(private="file")
_get_qualified_name :: proc(p: ^Parser, group: int, idx: ^u32) -> Result {
	idx^ += 1
	real_begin := idx^
	for ; idx^ < u32(len(p.text)) && p.text[idx^] != ']'; idx^ += 1 {}

	if idx^ >= u32(len(p.text)) {
		return lex_error(p, idx^, "unmatched '['")
	}

	idx^ += 1

	real_end := idx^ - 1
	if real_begin == idx^ {
		real_end = real_begin
	}

	append(&p.tokens, Token {
		    type = .Query_Name,
		    group=u16(group),
		    begin = real_begin,
		    len = real_end-real_begin })

	return .Ok
}

@(private="file")
_get_numeric :: proc(p: ^Parser, group: int, idx: ^u32) -> Result {
	/* TODO hex check here ? */

	begin := idx^
	is_float: bool

	for ; idx^ < u32(len(p.text)) &&
	    (unicode.is_digit(rune(p.text[idx^])) || p.text[idx^] == '.'); idx^ += 1 {
		if p.text[idx^] == '.' {
			if is_float {
				return lex_error(p, idx^, "malformed decimal")
			}
			is_float = true
		}
	}

	type := Token_Type.Literal_Int
	if is_float {
		type = .Literal_Float
	}

	append(&p.tokens, Token {
		    type=type,
		    group=u16(group),
		    begin=begin,
		    len=idx^-begin })

	return .Ok
}

@(private="file")
_get_variable :: proc(p: ^Parser, group: int, idx: ^u32) {
	begin := idx^
	idx^ += 1

	for ; p.text[idx^] == '_' ||
	      unicode.is_digit(rune(p.text[idx^])) ||
	      unicode.is_alpha(rune(p.text[idx^])); idx^ += 1 {
	}

	append(&p.tokens, Token {
		    type=.Query_Variable,
		    group=u16(group),
		    begin=begin,
		    len=idx^-begin })
}

@(private="file")
_get_block_comment :: proc(p: ^Parser, group: int, idx: ^u32) -> Result {

	begin := idx^

	for ; idx^+1 < u32(len(p.text)) &&
	    !(p.text[idx^] == '*' && p.text[idx^+1] == '/'); idx^ += 1 {
		if p.text[idx^] == '\n' {
			append(&p.lf_vec, idx^)
		}
	}

	if idx^+1 >= u32(len(p.text)) {
		return lex_error(p, idx^, "unmatched `/*'")
	}

	idx^ += 2

	append(&p.tokens, Token {
		    type=.Query_Comment,
		    group=u16(group),
		    begin=begin,
		    len=idx^-begin })

	return .Ok
}

@(private="file")
_get_line_comment :: proc(p: ^Parser, group: int, idx: ^u32) -> Result {
	begin := idx^
	offset := strings.index_byte(p.text[idx^:], '\n')
	if offset == -1 {
		idx^ = u32(len(p.text))
	} else {
		idx^ += u32(offset)
		append(&p.lf_vec, idx^)
		idx^ += 1
	}

	append(&p.tokens, Token {
		    type=.Query_Comment,
		    group=u16(group),
		    begin=begin,
		    len=idx^-begin })

	return .Ok
}

@(private="file")
_get_symbol :: proc(p: ^Parser, group: int, idx: ^u32) -> Result {
	begin := idx^
	//idx^ += 1

	type: Token_Type
	ret: bool

	/* Check for 2 character symbols first */
	if begin < u32(len(p.text)) {
		type, ret = bytemap.get(&p.tok_map, p.text[begin:begin+2])
	}

	if ret {
		#partial switch type {
		case .Sym_Line_Comment:
			return _get_line_comment(p, group, idx)
		case .Sym_Block_Comment:
			return _get_block_comment(p, group, idx)
		}
		idx^ += 2
	} else {
		idx^ += 1
		type, ret = bytemap.get(&p.tok_map, p.text[begin:idx^])
	}

	if !ret {
		return lex_error(p, idx^, "invalid symbol")
	}

	append(&p.tokens, Token {
		    type=type,
		    group=u16(group),
		    begin=begin,
		    len=idx^-begin })

	return .Ok
}

@(private = "file")
_is_symbol :: proc(c: u8) -> bool {
	return strings.index_byte("#()!=+-*/%~|&^.<>,;", c) >= 0
}

@(private = "file")
_lex_tokenize :: proc(p: ^Parser) -> Result {
	i : u32 = 0
	append(&p.tokens, Token { type=.Query_Begin })

	group : int
	group_stack : [dynamic]int
	append(&group_stack, group)
	defer delete(group_stack)

	ret := 0

	for ret == 0 && i < u32(len(p.text)) {
		tok_len := 0
		switch {
		case unicode.is_space(rune(p.text[i])):
			_skip_whitespace(p, &i)
		case p.text[i] == '\'':
			_get_string(p, group, &i)
		case p.text[i] == '[':
			_get_qualified_name(p, group, &i) or_return
		case unicode.is_digit(rune(p.text[i])) ||
		    (i+1 < u32(len(p.text)) && unicode.is_digit(rune(p.text[i]))):
			_get_numeric(p, group, &i) or_return
		case p.text[i] == '@':
			_get_variable(p, group, &i)
		case p.text[i] == '(':
			group += 1
			bit_array.set(&p.consumed, len(p.tokens))
			append(&group_stack, group)
			append(&p.tokens, Token {type=.Sym_Lparen, group=u16(group), begin=i, len=1})
			i += 1
		case p.text[i] == ')':
			if len(group_stack) == 1 {
				return lex_error(p, i, "unmatched ')'")
			}
			bit_array.set(&p.consumed, len(p.tokens))
			append(&p.tokens, Token {type=.Sym_Rparen, group=u16(group), begin=i, len=1})
			i += 1
			pop(&group_stack)
			group = group_stack[len(group_stack)-1]
		case _is_symbol(p.text[i]):
			_get_symbol(p, group, &i) or_return
		case p.text[i] == '_' ||
		    unicode.is_digit(rune(p.text[i])) ||
		    unicode.is_alpha(rune(p.text[i])):
			_get_name(p, group, &i)
		case:
			return lex_error(p, i)
		}
	}

	if len(group_stack) > 1 {
		return lex_error(p, i, "unmatched '('")
	}

	append(&p.tokens, Token { type = .Query_End })

	/* Dump tokens */
	//for tok in p.tokens {
	//	if enum_name, ok := fmt.enum_value_to_string(tok.type); ok {
	//		if tok.len > 0 {
	//			fmt.println(enum_name, p.text[tok.begin:tok.begin+tok.len])
	//		} else {
	//			fmt.println(enum_name)
	//		}
	//	}
	//}

	return .Ok
}

@(test)
lex_error_check :: proc(t: ^testing.T) {
	p := make_parser()

	/* Unmatched tokens */
	p.text = "select a,b,c,[ntll from foo where 1=1"
	testing.expect_value(t, lex_lex(&p), Result.Error)

	p.text = "select a,b,c,ntll] from foo where 1=1"
	testing.expect_value(t, lex_lex(&p), Result.Error)

	p.text = "select 124+35*24 / (124-2 from [foo] where 1<>2"
	testing.expect_value(t, lex_lex(&p), Result.Error)

	p.text = "select 124+35*24 / (124-2))"
	testing.expect_value(t, lex_lex(&p), Result.Error)

	p.text = "select /* a comment * / 1,2 from foo"
	testing.expect_value(t, lex_lex(&p), Result.Error)

	/* Will throw p error as multiply, divide */
	//p.text = "select / * a comment */ 1,2 from foo"
	//testing.expect_value(t, lex_lex(&p), Result.Error)

	/* Illegal symbols */
	p.text = "select $var from foo join foo on 1=1"
	testing.expect_value(t, lex_lex(&p), Result.Error)

	p.text = "select `col` from foo join foo on 1=1"
	testing.expect_value(t, lex_lex(&p), Result.Error)

	/* Malformed number */
	p.text = "select 1234.1234.1234 from foo join foo on 1=1"
	testing.expect_value(t, lex_lex(&p), Result.Error)

	/* Oh shit this is actually legal in SQL Server... */
	//p.text = "select 1234shnt from foo join foo on 1=1"
	//testing.expect_value(t, lex_lex(&p), Result.Error)

	destroy_parser(&p)
}

@(test)
lex_check :: proc(t: ^testing.T) {
	p := make_parser()

	/* For the following tests...
	 * len(p.tokens) = token_count + 2
	 * This is because every query begins and ends
	 * with .Query_Begin and .Query_End
	 */

	//      01      2   3    4   5    6   7  890 1
	p.text = "select col from foo join foo on 1=1"
	testing.expect_value(t, lex_lex(&p), Result.Ok)
	testing.expect_value(t, len(p.tokens), 12)
	testing.expect_value(t, p.tokens[0].type, Token_Type.Query_Begin)
	testing.expect_value(t, p.tokens[1].type, Token_Type.Select)
	testing.expect_value(t, p.tokens[2].type, Token_Type.Query_Name)
	testing.expect_value(t, p.tokens[3].type, Token_Type.From)
	testing.expect_value(t, p.tokens[4].type, Token_Type.Query_Name)
	testing.expect_value(t, p.tokens[5].type, Token_Type.Join)
	testing.expect_value(t, p.tokens[6].type, Token_Type.Query_Name)
	testing.expect_value(t, p.tokens[7].type, Token_Type.On)
	testing.expect_value(t, p.tokens[8].type, Token_Type.Literal_Int)
	testing.expect_value(t, p.tokens[9].type, Token_Type.Sym_Eq)
	testing.expect_value(t, p.tokens[10].type, Token_Type.Literal_Int)
	testing.expect_value(t, p.tokens[11].type, Token_Type.Query_End)

	//0      1
	//      2      34 5 6 7 8 9 01 234567 8
	//      9    0   1
	//23
	//4  56      78 9
	p.text = `
	select (
		select (1 + 2 * 5 + (33-(1))) [bar baz]
		from foo f
	) f
	from (select 1) x
	`
	testing.expect_value(t, lex_lex(&p), Result.Ok)
	testing.expect_value(t, len(p.tokens), 32)

	destroy_parser(&p)
}

