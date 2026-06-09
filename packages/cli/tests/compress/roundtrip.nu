use ../../test.nu *

# Compressing a blob and then decompressing it returns the original blob across each supported format.

let server = spawn

let blob = "hello, world!\n" | tg write

# Compress and decompress the blob with each format, then compare the result to the original by id.
def roundtrip [format: string] {
	let compressed = tg compress --format $format $blob | str trim
	assert ($compressed != $blob) $"the compressed blob should differ from the original for format=($format)"
	let decompressed = tg decompress $compressed | str trim
	assert equal $decompressed $blob $"the decompressed blob should equal the original for format=($format)"
}

roundtrip bz2
roundtrip gz
roundtrip xz
roundtrip zst

# The decompressed contents read back as the original text.
let compressed = tg compress --format gz $blob | str trim
let decompressed = tg decompress $compressed | str trim
let text = tg read $decompressed
assert equal $text "hello, world!" "the decompressed contents should match the original text"
