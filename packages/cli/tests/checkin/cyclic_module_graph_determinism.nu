# This test verifies that checking in different files from a cyclic module graph produces the same graph ID regardless of which file is checked in first.

use ../../test.nu *

let server = spawn

let path = artifact {
	a.tg.ts: 'import "./b.tg.ts";'
	b.tg.ts: 'import "./a.tg.ts";'
}

# Check in a.tg.ts first.
let a_id = tg checkin ($path | path join 'a.tg.ts')
let a_obj = tg get $a_id
let a_graph_id = $a_obj | parse --regex '"graph":(gph_[a-z0-9]+)' | get capture0 | first

# Check in b.tg.ts second.
let b_id = tg checkin ($path | path join 'b.tg.ts')
let b_obj = tg get $b_id
let b_graph_id = $b_obj | parse --regex '"graph":(gph_[a-z0-9]+)' | get capture0 | first

# The graph IDs should be the same since both files are part of the same cycle.
assert equal $a_graph_id $b_graph_id
