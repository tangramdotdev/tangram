use ../../test.nu *

# The cpu and memory options are reflected when getting the sandbox.

let server = spawn

let id = tg sandbox create --cpu 1 --memory 268435456 | str trim

let sandbox = tg sandbox get $id | from json
assert equal $sandbox.cpu 1 "the cpu option should be reflected"
assert equal $sandbox.memory 268435456 "the memory option should be reflected"
assert equal $sandbox.status "started" "the sandbox should be started"

tg sandbox destroy $id
