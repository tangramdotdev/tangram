use ../../test.nu *

# The format flag selects the compression encoding, as evidenced by each format's magic bytes.

let server = spawn

let blob = "hello, world!\n" | tg write

# Compress the blob with the given format and assert that the output begins with the expected magic bytes.
def magic [format: string, expected: string] {
	let compressed = tg compress --format $format $blob | str trim
	let hex = tg read $compressed | into binary | encode hex
	assert ($hex | str starts-with $expected) $"the compressed blob should begin with the magic bytes for format=($format)"
}

magic bz2 "425A68"
magic gz "1F8B"
magic xz "FD377A585A00"
magic zst "28B52FFD"
