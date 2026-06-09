use ../../test.nu *

# Compressing or decompressing a well formed blob id that does not exist fails.

let server = spawn

let id = "blb_010000000000000000000000000000000000000000000000000000"

let compress_output = tg compress --format gz $id | complete
failure $compress_output

let decompress_output = tg decompress $id | complete
failure $decompress_output
