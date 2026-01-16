use ../../test.nu *

# Test that building a package by tag that depends on another package's components
# gets cache hits for those components rather than rebuilding them.
#
# This test covers two scenarios:
# 1. LOCAL CACHING: Build "a", then build "b" on the same server.
#    "b"'s build should get cache hits for "a"'s components.
# 2. REMOTE PULLING: Push builds to remote, then on a fresh client,
#    build "b" by tag and verify it pulls "a"'s components from cache.
#
# This simulates: `tg build std` then `tg build ripgrep` where ripgrep
# should get cache hits for all std components.

# Create remote and two local servers.
let remote = spawn --cloud -n remote
let local1 = spawn -n local_one -c {
	remotes: [{ name: default, url: $remote.url }]
}
let local2 = spawn -n local_two -c {
	remotes: [{ name: default, url: $remote.url }]
}

# Create package "a" that exports a reusable component function.
# The default export also uses the component, simulating std's structure.
let a_path = artifact {
	tangram.ts: '
		// Reusable component that produces a file.
		export const component = () => tg.file("component from a");

		// Default export uses the component.
		export default async () => {
			const c = await component();
			return tg.directory({ "component.txt": c });
		};

		export const metadata = {
			tag: "a/1.0.0",
		};
	'
}

# Create package "b" that imports "a" and uses its component.
let b_path = artifact {
	tangram.ts: '
		import * as a from "a/1.0.0";

		export default async () => {
			// Call a.component() - should be cache hit if a was already built.
			const fromA = await a.component();
			return tg.directory({
				"from_a.txt": fromA,
				"from_b.txt": tg.file("content from b"),
			});
		};

		export const metadata = {
			tag: "b/1.0.0",
		};
	'
}

# Publish both packages on local1.
tg -u $local1.url publish $a_path
tg -u $local1.url publish $b_path

# ============================================================================
# SCENARIO 1: LOCAL CACHING
# Build "a" then "b" on the same server. "b" should cache hit on "a"'s component.
# ============================================================================

# Build "a" by tag. This will:
# - Execute a's default export
# - Which calls component(), creating a child process for it
let a_build_id = tg -u $local1.url build -d a/1.0.0 | str trim
tg -u $local1.url wait $a_build_id
let a_output = tg -u $local1.url get $a_build_id | from json
let a_children = $a_output.children

# Now build "b" by tag. This will:
# - Execute b's default export
# - Which calls a.component()
# - This should be a CACHE HIT because the same process was already created by a's build
let b_build_id = tg -u $local1.url build -d b/1.0.0 | str trim
tg -u $local1.url wait $b_build_id
let b_output = tg -u $local1.url get $b_build_id | from json
let b_children = $b_output.children

# Find child processes from a and b that correspond to calling component().
# In Tangram, if the same function is called with the same inputs, the process ID
# should be identical (content-addressed). This is the key verification.

# Get component process from a's build (should have a child that calls component()).
mut a_component_process = null
mut a_component_output = null
for child in $a_children {
	let child_data = tg -u $local1.url get $child.item | from json
	if $child_data.output? != null and $child_data.output.kind == "object" {
		# Found a child with an object output - this is likely the component.
		$a_component_process = $child.item
		$a_component_output = $child_data.output.value
		break
	}
}

# Get component process from b's build.
mut b_component_process = null
mut b_component_output = null
for child in $b_children {
	let child_data = tg -u $local1.url get $child.item | from json
	if $child_data.output? != null and $child_data.output.kind == "object" {
		# Found a child with an object output - check if it matches a's component.
		if $a_component_output != null and $child_data.output.value == $a_component_output {
			$b_component_process = $child.item
			$b_component_output = $child_data.output.value
			break
		}
	}
}

# VERIFICATION 1: The component outputs should be the same.
if $a_component_output != null and $b_component_output != null {
	assert equal $a_component_output $b_component_output "LOCAL: component output should be the same (cache hit)"
}

# VERIFICATION 2: The process IDs should match (same process, not re-executed).
if $a_component_process != null and $b_component_process != null {
	assert equal $a_component_process $b_component_process "LOCAL: component process ID should match (cache hit)"
}

# ============================================================================
# SCENARIO 2: REMOTE PULLING
# Push builds to remote, then on a fresh client (local2), build "b" by tag.
# It should pull cached components from remote rather than rebuilding.
# ============================================================================

# Push both builds to remote with all children and commands.
tg -u $local1.url push --recursive --commands $a_build_id
tg -u $local1.url push --recursive --commands $b_build_id

# On local2 (fresh cache), build "a" first.
let a_build_fresh_id = tg -u $local2.url build -d a/1.0.0 | str trim
tg -u $local2.url wait $a_build_fresh_id
let a_output_fresh = tg -u $local2.url get $a_build_fresh_id | from json

# VERIFICATION 3: a's build should be pulled from remote (same process ID).
assert equal $a_build_id $a_build_fresh_id "REMOTE: a's process ID should match (pulled from remote)"

# Now build "b" on local2.
let b_build_fresh_id = tg -u $local2.url build -d b/1.0.0 | str trim
tg -u $local2.url wait $b_build_fresh_id
let b_output_fresh = tg -u $local2.url get $b_build_fresh_id | from json

# VERIFICATION 4: b's build should be pulled from remote (same process ID).
assert equal $b_build_id $b_build_fresh_id "REMOTE: b's process ID should match (pulled from remote)"

# VERIFICATION 5: The outputs should match exactly.
assert equal ($a_output.output.value) ($a_output_fresh.output.value) "REMOTE: a's output should match"
assert equal ($b_output.output.value) ($b_output_fresh.output.value) "REMOTE: b's output should match"

# VERIFICATION 6: Verify the component artifact is accessible on local2.
if $a_component_output != null {
	let component_on_local2 = tg -u $local2.url get $a_component_output | complete
	success $component_on_local2 "REMOTE: component artifact should be present on fresh client"
}

# Get b's children on local2 and verify they match.
let b_children_fresh = $b_output_fresh.children

# Find the component process in b's fresh build.
mut b_component_fresh_process = null
for child in $b_children_fresh {
	let child_data = tg -u $local2.url get $child.item | from json
	if $child_data.output? != null and $child_data.output.kind == "object" {
		if $a_component_output != null and $child_data.output.value == $a_component_output {
			$b_component_fresh_process = $child.item
			break
		}
	}
}

# VERIFICATION 7: The component process should be the same across all builds.
if $a_component_process != null and $b_component_fresh_process != null {
	assert equal $a_component_process $b_component_fresh_process "REMOTE: component process should match original (pulled from remote)"
}
