use ../../test.nu *

# Checking out a graph pointer reference fails with an error reporting that checking out graph pointers is unsupported.

let server = spawn

# Put a graph object and capture its id.
let graph = '
	tg.graph({
		"nodes": [
			{
				"kind": "directory",
				"entries": {}
			}
		]
	})
'
let graph_id = tg put $graph

# Attempt to check out a graph pointer reference. This should fail because
# checking out graph pointers is unsupported.
let checkout_path = mktemp --directory | path join 'checkout'
let output = tg checkout $"graph=($graph_id)&index=0&kind=directory" $checkout_path | complete
failure $output 'checking out graph pointers is unsupported'
