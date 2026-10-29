use ../../util/test.nu *

# Waiting on a cancelled process reports the cancellation as an error outcome.

let local = server spawn --config { advanced: { checkpoints: true } }

let path = artifact {
	tangram.ts: 'export default async function () { while (true) { await tg.sleep(1); } }',
}
let watch = tg checkpoint watch runner.process.start | from json | get watch
let spawned = tg build --detach --verbose $path | from json
tg checkpoint wait runner.process.start $watch 0 | ignore
tg checkpoint continue runner.process.start $watch 0
tg checkpoint unwatch runner.process.start $watch

tg cancel $spawned.process $spawned.lease

let outcome = tg wait $spawned.process | from json
assert ($outcome.exit == 1) "a cancelled process should wait with a nonzero exit"
assert (($outcome.error? | is-not-empty)) "a cancelled process should wait with an error"
