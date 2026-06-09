use ../../test.nu *

# Waiting on a cancelled process reports the cancellation as an error outcome.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async () => { while (true) { await tg.sleep(1); } };',
}
let spawned = tg build --detach --verbose $path | from json

wait_until { (tg status --timeout 0 $spawned.process | from json) == ["started"] } "the process should start"

tg cancel $spawned.process $spawned.lease

let outcome = tg wait $spawned.process | from json
assert ($outcome.exit == 1) "a cancelled process should wait with a nonzero exit"
assert (($outcome.error? | is-not-empty)) "a cancelled process should wait with an error"
