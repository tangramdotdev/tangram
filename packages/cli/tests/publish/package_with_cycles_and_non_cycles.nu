use ../../test.nu *

let remote = spawn -n remote
let local = spawn -n local -c {
	remotes: [{ name: default, url: $remote.url }]
}

# Create a complex graph with both cycles and non-cycles:
#
#              main
#            /      \
#        cycle-a   independent
#         /   \        /    \
#     cycle-b  \     leaf1  leaf2
#        /      \
#     leaf2    leaf1
#     (cycle + leaf deps)
#
# This tests that the publish ordering:
# 1. Handles cycles (cycle-a <-> cycle-b)
# 2. Handles acyclic dependencies (independent -> leaf1, leaf2)
# 3. Both packages in the cycle import leaves (cycle-a -> leaf1, cycle-b -> leaf2)
# 4. Publishes leaves before their dependents
# 5. Publishes everything in a valid order

let shared_path = artifact {
	leaf1: {
		tangram.ts: '
			export default () => "I am leaf1!";
			export let metadata = {
				tag: "test-leaf1/1.0.0",
			};
		'
	},
	leaf2: {
		tangram.ts: '
			export default () => "I am leaf2!";
			export let metadata = {
				tag: "test-leaf2/1.0.0",
			};
		'
	},
	independent: {
		tangram.ts: '
			import leaf1 from "test-leaf1" with { local: "../leaf1" };
			import leaf2 from "test-leaf2" with { local: "../leaf2" };
			export default () => `Independent using: ${leaf1()} and ${leaf2()}`;
			export let metadata = {
				tag: "test-independent/1.0.0",
			};
		'
	},
	cycle-a: {
		tangram.ts: '
			import cycleB from "test-cycle-b" with { local: "../cycle-b" };
			import leaf1 from "test-leaf1" with { local: "../leaf1" };
			export default () => `Cycle A using: ${cycleB()} and ${leaf1()}`;
			export let greeting = () => "Hello from Cycle A";
			export let metadata = {
				tag: "test-cycle-a/1.0.0",
			};
		'
	},
	cycle-b: {
		tangram.ts: '
			import * as cycleA from "test-cycle-a" with { local: "../cycle-a" };
			import leaf2 from "test-leaf2" with { local: "../leaf2" };
			export default () => `Cycle B using: ${cycleA.greeting()} and ${leaf2()}`;
			export let metadata = {
				tag: "test-cycle-b/1.0.0",
			};
		'
	},
	main: {
		tangram.ts: '
			import cycleA from "test-cycle-a" with { local: "../cycle-a" };
			import independent from "test-independent" with { local: "../independent" };
			export default () => `Main using: ${cycleA()} and ${independent()}`;
			export let metadata = {
				tag: "test-main/1.0.0",
			};
		'
	}
}

let main_path = $shared_path | path join main

# Publish the main package - this should handle both cycles and non-cycles.
cd $main_path
let output = tg publish . | complete
success $output

# Extract IDs from stderr.
let extract_id = {|package_name: string|
	let line = $output.stderr | lines | find --regex $"info tagged ($package_name)" | get 0?
	if $line == null {
		error make { msg: $"($package_name) should be published." }
	}
	let id_part = $line | parse --regex 'dir_([a-z0-9]+)' | get capture0.0?
	if $id_part == null {
		error make { msg: $"Failed to extract ID for ($package_name)." }
	}
	$"dir_($id_part)"
}

let leaf1_id = do $extract_id "test-leaf1/1.0.0"
let leaf2_id = do $extract_id "test-leaf2/1.0.0"
let independent_id = do $extract_id "test-independent/1.0.0"
let cycle_a_id = do $extract_id "test-cycle-a/1.0.0"
let cycle_b_id = do $extract_id "test-cycle-b/1.0.0"
let main_id = do $extract_id "test-main/1.0.0"

# Verify all packages are tagged correctly on both servers.
let packages = [
	["tag", "id"];
	["test-leaf1/1.0.0", $leaf1_id],
	["test-leaf2/1.0.0", $leaf2_id],
	["test-independent/1.0.0", $independent_id],
	["test-cycle-a/1.0.0", $cycle_a_id],
	["test-cycle-b/1.0.0", $cycle_b_id],
	["test-main/1.0.0", $main_id]
]

for package in $packages {
	let tag = $package.tag
	let id = $package.id

	# Verify tag on local.
	let local_tag = run tg tag get $tag | from json | get item
	assert equal $local_tag $id $"Local tag for ($tag) does not match expected ID."

	# Verify tag on remote.
	let remote_tag = run tg --url $remote.url tag get $tag | from json | get item
	assert equal $remote_tag $id $"Remote tag for ($tag) does not match expected ID."

	# Verify object synced.
	let local_obj = run tg object get $id
	let remote_obj = run tg --url $remote.url object get $id
	assert equal $local_obj $remote_obj $"Object for ($tag) not synced between local and remote."
}

# Index servers.
run tg --url $remote.url index
run tg index

# Verify metadata is synced for all packages.
for package in $packages {
	let tag = $package.tag
	let id = $package.id

	let local_metadata = run tg object metadata $id | from json
	let remote_metadata = run tg --url $remote.url object metadata $id | from json
	assert equal $local_metadata $remote_metadata $"Metadata for ($tag) not synced between local and remote."
}
