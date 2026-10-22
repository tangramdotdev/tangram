use ../lib/test.nu *

# A sandbox spawn failure fails the process and finishes its log.

let local = server spawn

let id = tg build --no-tokens --detach --executable /tangram-missing-executable | referent node
let output = tg output $id | complete
failure $output
assert ($output.stderr | str contains 'failed to spawn the process in the sandbox')

# Log readers must receive EOF even though the executable never started.
let output = timeout 10 tg log --no-timeout $id | complete
success $output
assert equal $output.stdout ''
assert equal $output.stderr ''
