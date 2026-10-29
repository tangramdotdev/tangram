use ../../util/test.nu *

# A checkpoint wait with a timeout fails when the hit does not occur in time.

let local = server spawn --config { advanced: { checkpoints: true } }
let watch = tg checkpoint watch process.wait.attach | from json | get watch
let output = tg checkpoint wait --timeout 1ms process.wait.attach $watch 0 | complete
failure $output
assert ($output.stderr | str contains 'timed out waiting for the checkpoint hit') $output.stderr
