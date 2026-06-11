use ../../test.nu *

# Archiving and extracting an empty directory as tar roundtrips to the original directory.

let server = spawn

let dir = tg put 'tg.directory({})' | str trim

let blob = tg archive --format tar $dir | str trim
let extracted = tg extract $blob | str trim
assert equal $extracted $dir "the extracted directory should equal the original"
