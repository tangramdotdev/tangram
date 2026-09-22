use ../lib/test.nu *

# A sandbox spawn failure finishes the process log without reporting a control or sandbox task failure.

let server = server spawn --config { tracing: { stderr_format: 'json' } }

let id = tg build --detach --executable /tangram-missing-executable | str trim
let output = tg output $id | complete
failure $output
assert ($output.stderr | str contains 'failed to spawn the process in the sandbox')

# Log readers must receive EOF even though the executable never started.
let output = timeout 10 tg log --no-timeout $id | complete
success $output
assert equal $output.stdout ''
assert equal $output.stderr ''

snapshot (server_errors $server) ''
