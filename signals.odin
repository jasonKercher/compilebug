//+private
package streamql

import "linkedlist"

_global_remove_list: ^linkedlist.Node(string)
_signals_set: bool

signals_init :: proc() {
	if _signals_set {
		return
	}

	not_implemented()

	_signals_set = true
}
