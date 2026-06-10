use ../../test.nu *

# A sandbox created with a ttl is destroyed after the ttl expires.

let server = spawn

let id = tg sandbox create --ttl 1s | str trim

let output = tg sandbox get $id | complete
success $output

wait_until { (tg sandbox get $id | complete | get exit_code) != 0 } --timeout 15sec "the sandbox should expire"
let output = tg sandbox get $id | complete
failure $output
assert ($output.stderr | str contains "failed to find the sandbox") "the sandbox should be gone after its ttl"
