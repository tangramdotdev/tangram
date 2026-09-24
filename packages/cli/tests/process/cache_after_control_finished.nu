use ../lib/test.nu *

# A cache lookup uses finished runner state when the index is stale and process control has retired.

let server = server spawn --config {
	advanced: { checkpoints: true },
	runner: { process_state_ttl: 0 },
}
let finish_watch = tg checkpoint watch runner.process.finish | from json | get watch
let control_watch = tg checkpoint watch runner.process.control.finished | from json | get watch
let path = artifact { tangram.ts: 'export default () => "done";' }
let first = tg build --detach --verbose $path | from json
let process = $first.process | split row '?' | first
timeout 30s tg checkpoint wait runner.process.finish $finish_watch 0 | ignore
tg index

let batch_watch = tg checkpoint watch index.batch --params '{"finished_process":true}' | from json | get watch
tg checkpoint unwatch runner.process.finish $finish_watch
timeout 30s tg checkpoint wait index.batch $batch_watch 0 | ignore
timeout 30s tg checkpoint wait runner.process.control.finished $control_watch 0 | ignore

let output = timeout 10s tg build --detach --verbose $path | complete
success $output "the finished cached process must not require a live control channel"
let second = $output.stdout | from json
assert equal $second.cached true
assert equal ($second.process | split row '?' | first) $process

tg checkpoint unwatch runner.process.control.finished $control_watch
tg checkpoint unwatch index.batch $batch_watch
success (tg wait $second.process | complete)
