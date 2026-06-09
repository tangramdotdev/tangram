use ../../test.nu *

# A server without a runner omits the sandbox permits from the processes health.

let server = spawn --config { runner: false }

let health = tg health --fields processes | from json
assert equal ($health.processes | columns) [created started] "the processes health should omit the permits"
