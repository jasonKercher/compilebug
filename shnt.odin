package streamql

import "fifo"

main :: proc()
{
	root_fifo_vec := make([dynamic]fifo.Fifo(^Record))

	process: Process
	process_activate(&process, &root_fifo_vec, 0)
	
	for f in &root_fifo_vec {
		fifo.set_size(&f, 1)
	}
}

process_activate :: proc(process: ^Process, root_fifo_vec: ^[dynamic]fifo.Fifo(^Record),  base_size: int) {
	/*** IF THIS IS NOT NESTED, NO BUG ***/
	_new_fifo_root :: proc(root_fifo_vec: ^[dynamic]fifo.Fifo(^Record)) -> ^fifo.Fifo(^Record) {
		return nil
	}
	/***  ***/
	if process.input == nil {
		process.input = fifo.new_fifo(^Record, u16(base_size))
		fifo.advance(process.input)
	}
}

Record :: struct {fields: []string}
Process :: struct {input: ^fifo.Fifo(^Record)}
