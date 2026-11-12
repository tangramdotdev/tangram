use ../../util/test.nu *

# A stopped sandbox is destroyed once its ttl passes, counted from its last use.

let local = server spawn --now '2026-01-01T00:00:00Z' --config { runner: { stopped_sandbox_interval: 1 } }

let sandbox = tg sandbox create --no-tokens --ttl 5s | referent node
tg sandbox stop --wait $sandbox
let data = tg sandbox get $sandbox | from json | get data
assert equal $data.status stopped
assert ($data.expires_at? | is-not-empty) "the stopped sandbox should record when it expires"

# Advance the clock past the ttl, and wait for the sandbox to be destroyed.
advance_time $local 6sec
success (timeout 30s tg sandbox wait $sandbox | complete) "the stopped sandbox should be destroyed after its ttl"
assert equal (tg sandbox get $sandbox | from json | get data.status) destroyed
