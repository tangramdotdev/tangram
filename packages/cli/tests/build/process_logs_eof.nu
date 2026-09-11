use ../../test.nu *

for compaction in [false true] {
	let server = server spawn --config { indexer: { log_compaction: $compaction } }
	let path = artifact {
		tangram.ts: '
			export default function () {
				console.log("abc");
				console.error("defghi");
			}
		'
	}
	let id = tg build --detach $path | str trim
	tg wait $id | ignore
	let output = timeout 10 tg log --no-timeout $id | complete
	success $output
	if $compaction {
		tg index
	}

	# Forward the resolved window through another server as well.
	let reader = server spawn --name reader --config { remotes: { default: { url: $server.url } } }
	let output = timeout 10 tg --url $reader.url log --no-timeout --stream stdout --position end.96 --length=-98 $id | complete
	success $output
	assert equal $output.stdout "c\n"
	assert equal $output.stderr ""
	$env.TANGRAM_URL = $server.url

	# Intersect the requested window with EOF for combined and individual streams.
	for case in [
		{ streams: 'stdout,stderr', length: 11, stdout: "abc\n", stderr: "defghi\n" },
		{ streams: 'stdout', length: 4, stdout: "abc\n", stderr: "" },
		{ streams: 'stderr', length: 7, stdout: "", stderr: "defghi\n" },
	] {
		for position in [($case.length + 96 | into string) $"start.($case.length + 96)" 'end.96'] {
			let length = 0 - $case.length - 96
			let output = timeout 10 tg log --no-timeout --streams $case.streams --position $position $"--length=($length)" $id | complete
			success $output
			assert equal $output.stdout $case.stdout
			assert equal $output.stderr $case.stderr
		}
		let output = timeout 10 tg log --no-timeout --streams $case.streams --position end.96 --length=-95 $id | complete
		success $output
		assert equal $output.stdout ""
		assert equal $output.stderr ""
	}

	# Clipping at EOF must preserve the lower bound and the budget across windows.
	for case in [
		{ position: 'end.96', length: -98, size: 4096, expected: "c\n" },
		{ position: '100', length: -98, size: 1, expected: "\nc" },
		{ position: '4', length: -3, size: 2, expected: "c\nb" },
		{ position: '2', length: -10, size: 4096, expected: "ab" },
		{ position: 'end.-1', length: -2, size: 4096, expected: "bc" },
		{ position: 'end.0', length: -9223372036854775808, size: 2, expected: "c\nab" },
	] {
		let output = timeout 10 tg log --no-timeout --stream stdout --position $case.position $"--length=($case.length)" --size $case.size $id | complete
		success $output
		assert equal $output.stdout $case.expected
		assert equal $output.stderr ""
	}

	let path = artifact { tangram.ts: 'export default function () {}' }
	let id = tg build --detach $path | str trim
	tg wait $id | ignore
	for position in ['0' '100' 'end.0' 'end.96'] {
		let output = timeout 10 tg log --no-timeout --position $position --length=-100 $id | complete
		success $output
		assert equal $output.stdout ""
		assert equal $output.stderr ""
	}
}
