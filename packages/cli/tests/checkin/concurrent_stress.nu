use ../../test.nu *

# Stress test for concurrent directory checkins with varying sizes.
# Spawns 400 concurrent checkins across three size tiers to reproduce bottlenecks.

let server = spawn -c {
	index: {
		kind: 'lmdb',
		map_size: 524_288_000,
	},
	store: {
		kind: 'lmdb',
		map_size: 524_288_000,
	},
	tokio_single_threaded: false,
}

# Generate a directory artifact with the given number of files.
def make_artifact [prefix: string, num_files: int] {
	let entries = 0..($num_files - 1) | reduce --fold {} { |i, acc|
		$acc | insert $"file_($i).txt" $"($prefix)_file_($i)_content"
	}
	artifact $entries
}

# Create 400 artifacts across three size tiers.
# 100 small (5 files), 150 medium (50 files), 150 large (500 files).
# Total: ~83,000 unique files.
let small = 0..99 | each { |i| make_artifact $"small_($i)" 5 }
let medium = 0..149 | each { |i| make_artifact $"medium_($i)" 50 }
let large = 0..149 | each { |i| make_artifact $"large_($i)" 500 }
let all_paths = $small | append $medium | append $large

let start = date now

let outputs = $all_paths | par-each --threads 400 { |path|
	tg checkin --root $path | complete
}

let elapsed = (date now) - $start
print -e $"concurrent stress: 400 checkins completed in ($elapsed)"

for output in $outputs {
	success $output
}
