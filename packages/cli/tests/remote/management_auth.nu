use ../../test.nu *

let remote = spawn -n remote
let auth_enabled = spawn --config { authentication: true } -n auth-enabled

let output = tg remote put default $remote.url | complete
failure $output "An auth-enabled server should not allow remote management."
assert ($output.stderr | str contains "forbidden") "The error should mention that the request is forbidden."

let auth_disabled = spawn -n auth-disabled

tg remote put default $remote.url
tg remote delete default
