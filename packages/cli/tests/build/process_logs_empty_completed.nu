use ../../test.nu *

let server = server spawn --config { advanced: { checkpoints: true }, indexer: { log_compaction: false }, runner: { process_state_ttl: 0.01 } }
let watch = tg checkpoint watch runner.process.control.finished | from json | get watch
let path = artifact { tangram.ts: 'export default function () {}' }
let id = tg build --detach $path | str trim
tg wait $id | ignore
timeout 10 tg checkpoint wait runner.process.control.finished $watch 0 | ignore
tg checkpoint continue runner.process.control.finished $watch 0
tg checkpoint unwatch runner.process.control.finished $watch

# Restart to discard every in-memory close notification.
let server = server restart $server
let output = timeout 10 tg log --no-timeout $id | complete
success $output
assert equal $output.stdout ""
assert equal $output.stderr ""
