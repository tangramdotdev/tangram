use ../../test.nu *

# Each supported algorithm produces a checksum tagged with the algorithm name and the expected digest length.

let server = spawn

let blob = "hello, world!\n" | tg write

# The digest lengths are in hex characters.
def check [algorithm: string, length: int] {
	let checksum = tg checksum --algorithm $algorithm $blob | from json
	assert ($checksum | str starts-with $"($algorithm):") $"the checksum should be tagged with ($algorithm)"
	let digest = $checksum | str replace $"($algorithm):" ""
	assert equal ($digest | str length) $length $"the ($algorithm) digest should have ($length) hex characters"
}

check blake3 64
check sha256 64
check sha512 128
