use ../../util/test.nu *

# Waiting with --until for a status that the sandbox already has returns at once, and the status without --until prints the current status.

let local = server spawn

let sandbox = tg sandbox create --wait --no-tokens | referent node
let output = tg sandbox status $sandbox | from json
assert equal $output [started] "the status should be the current status"
let output = tg sandbox status --until started $sandbox | from json
assert equal $output [started] "waiting for the current status should return at once"

# A stopped sandbox can be waited on until it is stopped.
tg sandbox stop $sandbox
let output = tg sandbox status --until stopped $sandbox | from json
assert equal ($output | last) stopped "waiting for the sandbox to stop should end with stopped"
let output = tg sandbox status --until stopped $sandbox | from json
assert equal $output [stopped] "waiting for the current status should return at once"
tg sandbox destroy $sandbox
