use ../../test.nu *

# Compressing and decompressing a file roundtrips to the original file, as documented by both commands.

let server = spawn

let file_id = tg put 'tg.file("contents")' | str trim
assert ($file_id | str starts-with "fil_") "the put should return a file id"

let compressed = tg compress --format gz $file_id | str trim
assert ($compressed | str starts-with "fil_") "the compressed output should be a file"
assert ($compressed != $file_id) "the compressed file should differ from the original"

let decompressed = tg decompress $compressed | str trim
assert equal $decompressed $file_id "the decompressed file should equal the original"
