use ../../lib/test.nu *

# Publishing a package whose dependency graph contains both an import cycle and acyclic dependencies tags every package in a valid order on both servers, syncs the objects, and produces matching metadata.

let remote = server spawn --cloud --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
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
			export default function () { return "I am leaf1!"; }
			export let metadata = {
				tag: "test-leaf1/1.0.0",
			};
		'
	},
	leaf2: {
		tangram.ts: '
			export default function () { return "I am leaf2!"; }
			export let metadata = {
				tag: "test-leaf2/1.0.0",
			};
		'
	},
	independent: {
		tangram.ts: '
			import leaf1 from "test-leaf1" with { source: "../leaf1" };
			import leaf2 from "test-leaf2" with { source: "../leaf2" };
			export default function () { return `Independent using: ${leaf1()} and ${leaf2()}`; }
			export let metadata = {
				tag: "test-independent/1.0.0",
			};
		'
	},
	cycle-a: {
		tangram.ts: '
			import cycleB from "test-cycle-b" with { source: "../cycle-b" };
			import leaf1 from "test-leaf1" with { source: "../leaf1" };
			export default function () { return `Cycle A using: ${cycleB()} and ${leaf1()}`; }
			export function greeting() { return "Hello from Cycle A"; }
			export let metadata = {
				tag: "test-cycle-a/1.0.0",
			};
		'
	},
	cycle-b: {
		tangram.ts: '
			import * as cycleA from "test-cycle-a" with { source: "../cycle-a" };
			import leaf2 from "test-leaf2" with { source: "../leaf2" };
			export default function () { return `Cycle B using: ${cycleA.greeting()} and ${leaf2()}`; }
			export let metadata = {
				tag: "test-cycle-b/1.0.0",
			};
		'
	},
	main: {
		tangram.ts: '
			import cycleA from "test-cycle-a" with { source: "../cycle-a" };
			import independent from "test-independent" with { source: "../independent" };
			export default function () { return `Main using: ${cycleA()} and ${independent()}`; }
			export let metadata = {
				tag: "test-main/1.0.0",
			};
		'
	}
}

let main_path = $shared_path | path join main

# Publish the main package - this should handle both cycles and non-cycles.
cd $main_path
let output = tg --no-quiet publish . | complete
success $output

# Extract IDs from stderr.
let extract_id = {|package_name: string|
	let line = $output.stderr | lines | find --regex $"info tagged ($package_name)" | get 0?
	if $line == null {
		error make { msg: $"($package_name) should be published." }
	}
	let id_part = $line | parse --regex 'dir_([a-z0-9]+)' | get 0.capture0?
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

# Read every package's tag and object from both servers concurrently.
let results = $packages | par-each --keep-order { |package|
	let local_object = tg object get --no-tokens $package.id
	let local_tag = tg tag get $package.tag | from json | get target.id
	let remote_object = tg --url $remote.url object get --no-tokens $package.id
	let remote_tag = tg --url $remote.url tag get $package.tag | from json | get target.id
	{ local_object: $local_object, local_tag: $local_tag, remote_object: $remote_object, remote_tag: $remote_tag }
}

# Verify that each package is tagged on both servers and that its object is synced.
for pair in ($packages | zip $results) {
	let package = $pair.0
	let result = $pair.1
	assert equal $result.local_tag $package.id $"Local tag for ($package.tag) does not match expected ID."
	assert equal $result.remote_tag $package.id $"Remote tag for ($package.tag) does not match expected ID."
	assert equal $result.local_object $result.remote_object $"Object for ($package.tag) not synced between local and remote."
}

# Index servers.
tg --url $remote.url index
tg index

# Read every package's metadata from both servers concurrently.
let results = $packages | par-each --keep-order { |package|
	let local = tg object metadata $package.id | from json
	let remote = tg --url $remote.url object metadata $package.id | from json
	{ local: $local, remote: $remote }
}

# Verify that the metadata is synced for all packages.
for pair in ($packages | zip $results) {
	assert equal $pair.1.local $pair.1.remote $"Metadata for ($pair.0.tag) not synced between local and remote."
}
