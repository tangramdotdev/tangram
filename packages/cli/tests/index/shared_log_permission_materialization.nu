use ../lib/test.nu *

# Shared log blobs should not cause redundant permission materialization on unrelated processes.

let local = server spawn --config {
	tracing: {
		filter: 'tangram=warn,tangram_index_fdb::update=debug,tangram_index_lmdb::update=debug'
		stderr_format: 'json'
	}
}

# Use distinct commands with identical empty logs to avoid reusing a cached process.
mut processes = []
for n in 1..4 {
	let process = tg spawn --sandbox --verbose --executable /bin/sh -- -c $'true # ($n)' | from json
	tg wait $process.process | ignore
	$processes = $processes | append $process.process
	success (timeout 60s tg index | complete) 'the index should catch up'
}
server stop $local

let permissions = open $local.log
	| lines
	| where ($it | str starts-with '{')
	| each { |line| $line | from json }
	| where { |event| $event.fields.message? == 'put a materialized permission' }
	| get fields

# Verify that the instrumentation captured an expected materialization before asserting the absence of redundant writes.
let first = $processes | first
assert ($permissions | any { |entry| $entry.resource == $first and $entry.subject == $first and $entry.permission == 'process_subtree' }) 'the trace should include the first process materializing its own subtree permission'

let redundant = $permissions
	| where { |entry| $entry.resource in $processes and $entry.subject in $processes and $entry.resource != $entry.subject }

assert equal ($redundant | length) 0 'shared logs should not materialize permissions on unrelated processes'
