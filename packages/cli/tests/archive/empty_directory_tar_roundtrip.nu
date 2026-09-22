use ../lib/test.nu *

# Archiving and extracting an empty directory as tar roundtrips to the original directory.

let server = server spawn

let dir = tg put --no-tokens 'tg.directory({})' | str trim

let blob = tg archive --format tar $dir | str trim
let extracted = tg extract --no-tokens $blob | str trim
assert equal $extracted $dir "the extracted directory should equal the original"
