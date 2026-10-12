use ../lib/test.nu *

# Shared log blobs should not cause redundant permission materialization on unrelated processes.

let local = server spawn --config {
	tracing: {
		filter: 'tangram=warn,tangram_index_lmdb::update=debug'
		stderr_format: 'json'
	}
}

# Use distinct commands with identical empty logs to avoid reusing a cached process.
let first = tg spawn --sandbox --verbose --executable /bin/sh -- -c 'true # first' | from json
tg wait $first.process
let second = tg spawn --sandbox --verbose --executable /bin/sh -- -c 'true # second' | from json
tg wait $second.process
success (timeout 60s tg index | complete) 'the index should catch up'
server stop $local

let permissions = open $local.log
	| lines
	| where ($it | str starts-with '{')
	| each { |line| $line | from json }
	| where { |event| $event.fields.message? == 'put a materialized permission' }
	| get fields

# Verify that the instrumentation captured an expected materialization before asserting the absence of redundant writes.
assert ($permissions | any { |entry| $entry.resource == $first.process and $entry.subject == $first.process and $entry.permission == 'process_subtree' }) 'the trace should include the first process materializing its own subtree permission'

let redundant = $permissions
	| where { |entry| $entry.resource == $first.process and $entry.subject == $second.process }
	| get permission
assert equal $redundant [] 'a shared log should not materialize permissions on an unrelated process'
