use ../lib/test.nu *

# Two unrelated processes must not gain permissions on each other. Both processes print nothing, so their logs are the same content-addressed empty blob, and the index materializes node log object permissions on the first process for the second process because the second process can read that blob. Every new process does this to every earlier process with the same log, so the index's work and size grow quadratically with the number of processes.

let local = server spawn --config {
	tracing: {
		filter: 'tangram=info,tangram_index_lmdb::update=debug'
		stderr_format: 'json'
	}
}

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
	| where { |event| $event.fields.message? == 'put a materialized permission' and $event.fields.resource? == $first.process and $event.fields.subject? == $second.process }
	| get fields.permission
assert equal $permissions [] 'the second process should not gain permissions on the first process'
