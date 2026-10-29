use ../../util/test.nu *

# A request for an unknown process that is too old to be starting fails without waiting for the process's control service.

let server = server spawn --config { authentication: { users: { providers: { insecure: true } } } }
let mallory = tg login --verbose --name mallory | from json

# Create an ID whose UUIDv7 body has a timestamp an hour ago, which no process uses.
let milliseconds = ((date now) - 1hr | into int) // 1_000_000
let timestamp = $milliseconds | into binary --endian big | bytes at 2..
let random = random binary 10
let version = (($random | bytes at 0..0 | into int) bit-and 0x0f) bit-or 0x70 | into binary | bytes at 0..0
let variant = (($random | bytes at 2..2 | into int) bit-and 0x3f) bit-or 0x80 | into binary | bytes at 0..0
let body = bytes build $timestamp $version ($random | bytes at 1..1) $variant ($random | bytes at 3..)
let process = bytes build 0x[00 00 08 00] $body | tg id

# The request fails before the control service request would time out.
let start = date now
let output = tg --token $mallory.token get $process | complete
let elapsed = (date now) - $start
failure $output "the request for an unknown process should fail"
assert ($elapsed < 5sec) $"the request should fail quickly, but took ($elapsed)"
