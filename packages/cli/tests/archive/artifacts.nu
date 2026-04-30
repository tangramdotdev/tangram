use ../../test.nu *

let server = spawn

# Create a directory with files, a subdirectory, and symlinks.
let path = artifact {
	hello.txt: 'Hello, World!'
	subdirectory: {
		nested.txt: 'nested contents'
	}
	link: (symlink 'hello.txt')
}

let id = tg checkin $path | str trim

# Test each archive format and compression combination by archiving and extracting the artifact, then comparing the result to the original.
def roundtrip [format: string, compression?: string] {
	mut args = [--format $format]
	if $compression != null {
		$args = ($args | append [--compression $compression])
	}
	let blob_id = tg archive ...$args $id | str trim
	let extracted_id = tg extract $blob_id | str trim
	assert ($extracted_id == $id) $"roundtrip failed for format=($format) compression=($compression)"
}

# Tar without compression.
roundtrip tar

# Tar with each compression format.
roundtrip tar bz2
roundtrip tar gz
roundtrip tar xz
roundtrip tar zstd

# Zip without compression.
roundtrip zip

# Zip with each compression format.
roundtrip zip bz2
roundtrip zip gz
roundtrip zip xz
roundtrip zip zstd
