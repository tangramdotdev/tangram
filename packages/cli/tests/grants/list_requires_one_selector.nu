use ../../test.nu *

# Listing grants requires exactly one of a resource or a principal.

let server = spawn --config { authentication: true }

let alice = tg login --verbose alice | from json

let output = tg --token $alice.token grants list | complete
failure $output "listing grants without a selector should fail"
assert ($output.stderr | str contains "expected exactly one of a resource or a principal") "the error should mention that exactly one selector is required"
