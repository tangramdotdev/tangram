use ../../test.nu *

# Graph IDs must be identical regardless of entry point for cyclic imports.
#
# Two issues were identified:
#
# 1. External pointers (references to nodes outside the SCC) used ephemeral
#    checkin graph indices, causing initial labels to differ by entry point.
#    Fix: Resolve external pointers to object IDs in checkin_graph_node_initial_label.
#
# 2. Symmetric nodes (identical content and structure) produce identical WL labels.
#    Fix: Use node paths as a tiebreaker in the sort.

let server = spawn

# Minimal 3-node star cycle: hub imports a and b, both a and b import hub.
# Nodes a and b have identical content and structure, producing identical WL labels.
let path = artifact {
	a.tg.ts: 'import "./hub.tg.ts"; export default {};'
	b.tg.ts: 'import "./hub.tg.ts"; export default {};'
	hub.tg.ts: 'import "./a.tg.ts"; import "./b.tg.ts"; export default {};'
}

# Check in from hub first.
let hub_id = tg checkin ($path | path join 'hub.tg.ts')
let hub_obj = tg get $hub_id
let graph_from_hub = $hub_obj | parse --regex '"graph":(gph_[a-z0-9]+)' | get capture0 | first

# Check in from a.
let a_id = tg checkin ($path | path join 'a.tg.ts')
let a_obj = tg get $a_id
let graph_from_a = $a_obj | parse --regex '"graph":(gph_[a-z0-9]+)' | get capture0 | first

# The graph IDs should be identical regardless of entry point.
assert equal $graph_from_hub $graph_from_a
