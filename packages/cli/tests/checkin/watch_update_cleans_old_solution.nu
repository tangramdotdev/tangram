use ../../test.nu *

# Updating a watched dependency rebuilds its referrers and removes the previous solution.

let server = spawn --config {
	advanced: {
		checkpoints: true
	}
}

let a_1_0_path = artifact {
	tangram.ts: '// a 1.0.0'
}
tg tag -p a/1.0.0 $a_1_0_path

let path = artifact {
	tangram.ts: 'import a from "a/^1";'
}

def checkin_background [path: path, update?: string] {
	job spawn {
		let job_id = job id
		let update = $update
		let output = if $update == null {
			tg checkin $path --watch --no-checkout-pointers --no-lock | complete
		} else {
			tg checkin $path --watch --no-checkout-pointers --no-lock --update $update | complete
		}
		$output | job send --tag $job_id 0
	}
}

# Record the graph size when the initial watch is published.
let initial_watch = (
	tg checkpoint watch checkin.watch.publish --params '{"updates":""}'
	| from json
	| get watch
)
let initial_checkin = checkin_background $path
let initial_hit = tg checkpoint wait checkin.watch.publish $initial_watch 0 | from json
tg checkpoint continue checkin.watch.publish $initial_watch 0
tg checkpoint unwatch checkin.watch.publish $initial_watch
let initial_output = job recv --tag $initial_checkin --timeout 10sec
success $initial_output
let initial_id = $initial_output.stdout | str trim

# Add a new candidate and update the dependency.
let a_1_1_path = artifact {
	tangram.ts: '// a 1.1.0'
}
tg tag -p a/1.1.0 $a_1_1_path

let update_watch = (
	tg checkpoint watch checkin.watch.publish --params '{"updates":"a"}'
	| from json
	| get watch
)
let update_checkin = checkin_background $path a
let update_hit = tg checkpoint wait checkin.watch.publish $update_watch 0 | from json
tg checkpoint continue checkin.watch.publish $update_watch 0
tg checkpoint unwatch checkin.watch.publish $update_watch
let update_output = job recv --tag $update_checkin --timeout 10sec
success $update_output
let update_id = $update_output.stdout | str trim

# The watched update must produce the same result as a cold checkin.
let cold_id = tg checkin $path --no-checkout-pointers --no-lock
assert ($update_id != $initial_id) "updating the dependency should change the artifact id"
assert ($update_id == $cold_id) "the watched update should match a cold checkin"

# Replacing one solution must not retain its unreachable artifact node.
let initial_nodes = $initial_hit.params.nodes
let update_nodes = $update_hit.params.nodes
assert ($update_nodes == $initial_nodes) $"the graph grew from ($initial_nodes) to ($update_nodes) nodes"
