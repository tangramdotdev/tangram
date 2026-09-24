use ../lib/test.nu *

let server = server spawn --config { advanced: { checkpoints: true } }

# Hold initialization and completion so EOF cannot find an indexed process.
let batch = tg checkpoint watch index.batch --params '{"started_process":true}' | from json | get watch
let finish = tg checkpoint watch runner.process.finish | from json | get watch
let end = tg checkpoint watch process.control.log.end | from json | get watch
let path = artifact {
	tangram.ts: '
		export default function () { return tg.build(child); }
		export function child() {}
	',
}
let id = tg build --detach $path | str trim
timeout 10s tg checkpoint wait index.batch $batch 0 | ignore

# EOF must reach persistence while the child's initial index write is pending.
let child = timeout 10s tg checkpoint wait process.control.log.end $end 0 | from json | get params.process
assert ($child != $id)
tg checkpoint unwatch process.control.log.end $end
tg checkpoint unwatch index.batch $batch
tg checkpoint unwatch runner.process.finish $finish
timeout 10s tg wait $id | ignore
success (timeout 10s tg index | complete)
assert ((tg get $child | from json | get log?) | is-not-empty)
