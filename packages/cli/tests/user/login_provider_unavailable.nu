use ../../test.nu *

# Requesting an unavailable authentication provider is rejected.

let server = spawn --config { authentication: { providers: { insecure: false } } }

let output = tg login --provider insecure alice | complete
failure $output "login should fail when the requested authentication provider is unavailable"
assert ($output.stderr | str contains "the requested authentication provider is not available") "the error should explain that the requested provider is unavailable"
