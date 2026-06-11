use ../../test.nu *

# Downloading a URL without a checksum fails, because the default checksum matches nothing and the caller must opt in with a wildcard.

skip_if_offline

let server = spawn

let output = tg download "http://www.example.com" | complete
failure $output
assert ($output.stderr | str contains "checksum mismatch") "the download should fail because no checksum was provided"
