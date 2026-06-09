use ../../test.nu *

# Cleaning fails when the server is not in single process mode.

let server = spawn --config { advanced: { single_process: false } }

let output = tg clean | complete
failure $output
assert ($output.stderr | str contains "cannot index in multi process mode") "the error should mention multi process mode"
