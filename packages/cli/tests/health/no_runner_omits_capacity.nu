use ../../test.nu *

# A server without a runner omits the capacity from the processes health.

let server = spawn --config { runner: false }

let health = tg health --fields processes | from json
assert equal ($health.processes | columns) [started] "the processes health should omit the capacity"
