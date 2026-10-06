use ../lib/test.nu *

# PUT and sync with log: null must preserve an existing compacted log.
let copy = server spawn --name copy --config { advanced: { checkpoints: true } }
let local = server spawn --name local --config { advanced: { checkpoints: true } }
tg remote put default $copy.url
let watch = tg checkpoint watch process.log.compact.read | from json | get watch
let path = artifact {
	tangram.ts: 'export default function () { console.log("stdout"); console.error("stderr"); }'
}
let process = tg build --no-tokens --detach $path | referent node
timeout 10s tg wait $process
let hit = timeout 10s tg checkpoint wait process.log.compact.read $watch 0 | from json
assert equal $hit.params.process $process
tg push --eager $process
let uncompacted = tg --url $copy.url get --no-tokens $process | from json
assert equal $uncompacted.log? null
tg checkpoint unwatch process.log.compact.read $watch
timeout 10s tg index
let log = tg get $process | from json | get log
assert ($log != null)

# Pause the push of the process with log: null after the destination requests it.
let receiver = server spawn --cloud --name receiver
tg --url $copy.url remote put default $receiver.url
let watch = tg --url $copy.url checkpoint watch sync.put.store.process | from json | get watch
let push_job = job spawn {
	let job_id = job id
	let output = tg --url $copy.url push --eager $process | complete
	$output | job send --tag $job_id 0
}
let hit = timeout 10s tg --url $copy.url checkpoint wait sync.put.store.process $watch 0 | from json
assert equal $hit.params.id $process

# Transfer the compacted log before allowing the process with log: null to arrive.
tg --url $local.url remote put default $receiver.url
tg --url $local.url push --eager --process-log-objects $process
assert equal (tg get $process | from json | get log) $log
tg --url $copy.url checkpoint unwatch sync.put.store.process $watch
success (job recv --tag $push_job --timeout 10sec)
assert equal (tg get $process | from json | get log?) $log
let output = timeout 10s tg log $process --no-timeout | complete
success $output
assert equal $output.stdout "stdout\n"
assert equal $output.stderr "stderr\n"

# The public PUT path applies the same index rule.
tg process put $process ($uncompacted | to json)
assert equal (tg get $process | from json | get log?) $log
let output = timeout 10s tg log $process --no-timeout | complete
success $output
assert equal $output.stdout "stdout\n"
assert equal $output.stderr "stderr\n"
