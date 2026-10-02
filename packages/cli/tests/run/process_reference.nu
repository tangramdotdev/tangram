use ../lib/test.nu *

# Process references reuse both inline and stored commands, with the new CLI options controlling execution.

let local = server spawn
let path = artifact {
	tangram.ts: 'export default () => { if (tg.process.env.VALUE === "fail") { throw new Error("failed"); } return tg.process.env.VALUE; };',
}
for operation in [spawn build] {
	let original = if $operation == spawn {
		tg spawn --network --env-string VALUE=original $path | str trim
	} else {
		tg build --detach --env-string VALUE=original $path | str trim
	}
	tg wait $original | ignore
	assert equal (tg output $original | from json) original
	let original_data = tg get $original | from json

	# A plain run does not inherit the original sandbox options.
	assert equal (tg run --env-string VALUE=run $original | from json) run

	# A spawn can explicitly reuse a sandbox chosen by the CLI.
	let sandbox = tg sandbox create | str trim
	let spawned = tg spawn $"--sandbox=($sandbox)" --env-string VALUE=spawn $original | str trim
	tg wait $spawned | ignore
	assert equal (tg output $spawned | from json) spawn
	let spawned_data = tg get $spawned | from json
	assert equal $spawned_data.sandbox ($sandbox | referent node)
	tg sandbox destroy $sandbox

	# A build creates its own sandbox and applies the CLI environment and cache options.
	let built = tg build --detach --cached=false --env-string VALUE=build $original | str trim
	tg wait $built | ignore
	assert equal (tg output $built | from json) build
	let built_data = tg get $built | from json
	assert ($built != $original)
	assert ($built_data.sandbox != $original_data.sandbox)
}

# A failed process can be used as a reference without inheriting its outcome.
let failed = tg spawn --env-string VALUE=fail $path | str trim
let outcome = tg wait $failed | from json
assert ($outcome.exit != 0)
assert equal (tg run --env-string VALUE=recovered $failed | from json) recovered
