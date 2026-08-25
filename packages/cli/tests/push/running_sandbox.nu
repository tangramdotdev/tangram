use ../../test.nu *

# A running sandbox cannot be pushed.

let remote = server spawn --cloud --name remote
let local = server spawn --name local
tg remote put default $remote.url

let sandbox = tg sandbox create | str trim
let output = tg push $sandbox | complete
failure $output
assert ($output.stderr | str contains "cannot sync a running sandbox")
