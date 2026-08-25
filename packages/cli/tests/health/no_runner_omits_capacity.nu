use ../../test.nu *

# A server without a runner omits the capacity from the processes health.

let server = server spawn --config { roles: [cleaner http indexer scheduler] }

let health = tg health --fields processes | from json
assert equal ($health.processes | columns) [started] "the processes health should omit the capacity"
