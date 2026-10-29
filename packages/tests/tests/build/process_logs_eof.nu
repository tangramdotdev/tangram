use ../../util/test.nu *

# Process log reads return the requested bytes at the end of a stream.

do {
	let local = server spawn
	let path = artifact {
		tangram.ts: '
			export default function () {
				console.log("abc");
				console.error("defghi");
			}
		'
	}
	let id = tg build --no-tokens --detach $path | referent node
	tg wait $id | ignore
	let output = timeout 10 tg log --no-timeout $id | complete
	success $output
	let path = artifact { tangram.ts: 'export default function () {}' }
	let empty = tg build --no-tokens --detach $path | referent node
	tg wait $empty | ignore

	# Forward the resolved window through another server as well.
	let local_reader = server spawn --name local-reader --config { remotes: { default: { url: $local.url } } }
	let output = timeout 10 tg --url $local_reader.url log --no-timeout --stream stdout --position end.96 --length=-98 $id | complete
	success $output
	assert equal $output.stdout "c\n"
	assert equal $output.stderr ""
	$env.TANGRAM_URL = $local.url

	# Intersect the requested window with EOF for combined and individual streams.
	let intersections = [
		{ streams: 'stdout,stderr', length: 11, stdout: "abc\n", stderr: "defghi\n" },
		{ streams: 'stdout', length: 4, stdout: "abc\n", stderr: "" },
		{ streams: 'stderr', length: 7, stdout: "", stderr: "defghi\n" },
	] | each { |case|
		let length = 0 - $case.length - 96
		let windows = [($case.length + 96 | into string) $"start.($case.length + 96)" 'end.96'] | each { |position|
			{ args: [--streams $case.streams --position $position $"--length=($length)"], id: $id, stderr: $case.stderr, stdout: $case.stdout }
		}
		let past = { args: [--streams $case.streams --position end.96 --length=-95], id: $id, stderr: "", stdout: "" }
		$windows | append $past
	} | flatten

	# Clipping at EOF must preserve the lower bound and the budget across windows.
	let clips = [
		{ position: 'end.96', length: -98, size: 4096, expected: "c\n" },
		{ position: '100', length: -98, size: 1, expected: "\nc" },
		{ position: '4', length: -3, size: 2, expected: "c\nb" },
		{ position: '2', length: -10, size: 4096, expected: "ab" },
		{ position: 'end.-1', length: -2, size: 4096, expected: "bc" },
		{ position: 'end.0', length: -9223372036854775808, size: 2, expected: "c\nab" },
	] | each { |case|
		{ args: [--stream stdout --position $case.position $"--length=($case.length)" --size $case.size], id: $id, stderr: "", stdout: $case.expected }
	}

	# Read an empty log from every position.
	let empties = ['0' '100' 'end.0' 'end.96'] | each { |position|
		{ args: [--position $position --length=-100], id: $empty, stderr: "", stdout: "" }
	}

	# Run the independent reads concurrently, then check each one.
	let cases = $intersections | append $clips | append $empties
	let outputs = $cases | par-each --keep-order { |case|
		timeout 10 tg log --no-timeout ...$case.args $case.id | complete
	}
	for pair in ($cases | zip $outputs) {
		let case = $pair.0
		let output = $pair.1
		success $output
		assert equal $output.stdout $case.stdout $"tg log ($case.args | str join ' ') should return the expected stdout"
		assert equal $output.stderr $case.stderr $"tg log ($case.args | str join ' ') should return the expected stderr"
	}
}
