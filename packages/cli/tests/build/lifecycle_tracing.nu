use ../lib/test.nu *

# Lifecycle events correlate nested successful and failed builds without recording their arguments.

let local = server spawn --config {
	tracing: {
		filter: 'tangram_server::process=debug,tangram_server::runner=debug,tangram_server::sandbox=debug,tangram_sandbox=debug'
		stderr_format: 'json'
	}
}
let path = artifact {
	tangram.ts: '
		export default async function (fail: string, secret: string) {
			return await tg.build(child, fail, secret);
		}
		export function child(fail: string, _secret: string) {
			if (fail === "true") throw new Error("intentional lifecycle failure");
			return tg.file("lifecycle success");
		}
	'
}
let secret = 'lifecycle-argument-must-not-be-logged'
let success_id = tg build --detach --no-tokens $path false $secret | referent node
assert equal (tg wait --no-tokens $success_id | from json | get exit) 0
let failure_id = tg build --detach --no-tokens $path true $secret | referent node
assert equal (tg wait --no-tokens $failure_id | from json | get exit) 1

wait_until {
	let log = open --raw $local.log
	($log | str contains 'destroyed the sandbox') and ($log | str contains 'received the process finish response')
} 'the lifecycle events should reach the server log'
server stop $local
let log = open --raw $local.log
assert not ($log | str contains $secret)
let events = $log | lines | where ($it | str starts-with '{') | each { from json }
for id in [$success_id $failure_id] {
	let started = $events | where { |event| $event.fields.message? == 'started the process' and $event.fields.process? == $id }
	assert equal ($started | length) 1
	let spans = $started | first | get spans | get name
	assert ('process.lifecycle' in $spans)
	assert ('process.run' in $spans)
	let finished = $events | where { |event| $event.fields.message? == 'collected the process output' and $event.fields.process? == $id }
	assert equal ($finished | length) 1
	let sandbox = $started | first | get fields.sandbox
	assert equal ($finished | first | get fields.sandbox) $sandbox
	assert ($events | any { |event| $event.fields.message? == 'initialized the process' and ($event.fields.parent? | default '' | str contains $id) })
	assert ($events | any { |event| $event.fields.message? == 'exited the process' and $event.fields.process? == $id })
}
assert ($events | any { |event| $event.fields.message? == 'collected the process output' and $event.fields.exit? == 0 })
assert ($events | any { |event| $event.fields.message? == 'collected the process output' and $event.fields.exit? == 1 })
