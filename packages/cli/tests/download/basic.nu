use ../../test.nu *

# Downloading a URL with a wildcard checksum returns a blob with the downloaded contents.

skip_if_offline

let server = spawn

let output = tg download "http://www.example.com" --checksum sha256:any | complete
success $output
assert ($output.stdout | str trim | str starts-with "blb_") "the download should return a blob id"

let contents = tg read ($output.stdout | str trim)
assert ($contents | str contains "Example Domain") "the blob should contain the downloaded page"
