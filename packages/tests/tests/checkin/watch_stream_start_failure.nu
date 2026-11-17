use ../../util/test.nu *

# Watched checkins reflect edits when an FSEvents stream fails to start or restart, and after its paths fall below the limit again. A single FSEvents stream accepts at most 4,096 paths, and a package that imports files from outside its root needs one path for the root and one for each imported file.

if $nu.os-info.name != 'macos' {
	skip_test 'this test requires macos'
}

let local = server spawn --config {
	advanced: { checkpoints: true },
	cache: { map_size: 268_435_456 },
	index: { map_size: 268_435_456 },
}

for existing_watch in [false true] {
	# Start with one external dependency, and optionally establish a working watch before exceeding the limit.
	let source = 'import "../external/0.txt" with { type: "file" }; export default () => "one";'
	let path = artifact {
		external: { '0.txt': "0\n" },
		package: { tangram.ts: $source },
	}
	let external = $path | path join external
	let package = $path | path join package
	if $existing_watch {
		tg checkin --no-tokens $package --watch | ignore
	}

	# Watch for the existing watch to apply the edit of the root module.
	let event = if $existing_watch {
		checkpoint watch watch.event --params { path: ($package | path join tangram.ts) }
	}

	# Import 4,100 external files to exceed the stream's path limit. Each module imports at most 900 files to stay under the object children limit.
	let count = 4100
	for index in 1..<$count {
		$"($index)\n" | save ($external | path join $"($index).txt")
	}
	let modules = 0..<$count | each { |index| $index } | chunks 900 | enumerate | each { |chunk|
		let name = $"module($chunk.index).tg.ts"
		let imports = $chunk.item | each { |index| $"import \"../external/($index).txt\" with { type: \"file\" };" }
		$imports | append 'export const value = 1;' | str join "\n" | save ($package | path join $name)
		$name
	}
	let imports = $modules | each { |name| $"import \"./($name)\";" } | str join "\n"
	$"($imports)\nexport default \(\) => \"one\";\n" | save --force ($package | path join tangram.ts)

	# Check in the expanded package after the existing watch applies the edit of the root module, which was written after the modules. The watched checkin fails to start the stream and discards the watch's graph.
	if $existing_watch {
		checkpoint wait $event | ignore
		checkpoint unwatch $event
	}
	let first = tg checkin --no-tokens $package --no-watch | referent node
	let watched = tg checkin --no-tokens $package --watch | referent node
	assert ($watched == $first) "the watched checkin should include the expanded imports"

	# Edit the root module while the stream cannot start.
	$"($imports)\nexport default \(\) => \"two\";\n" | save --force ($package | path join tangram.ts)
	let plain = tg checkin --no-tokens $package --no-watch | referent node
	assert ($plain != $first) "the edit should change the checked in package"
	let watched = tg checkin --no-tokens $package --watch | referent node
	assert ($watched == $plain) "the watched checkin should reflect the edit while the stream cannot start"

	# Remove the extra modules and imports so the stream can start again.
	for name in $modules {
		rm ($package | path join $name)
	}
	$source | save --force ($package | path join tangram.ts)
	let recovered = tg checkin --no-tokens $package --watch | referent node
	assert equal $recovered (tg checkin --no-tokens $package --no-watch | referent node) "the recovered checkin should reflect the reduced imports"

	# Edit the remaining external dependency after recovery, and wait for the watch to apply the edit.
	let event = checkpoint watch watch.event --params { path: ($external | path join '0.txt') }
	'after the recovery' | save --force ($external | path join '0.txt')
	checkpoint wait $event | ignore
	checkpoint unwatch $event
	let plain = tg checkin --no-tokens $package --no-watch | referent node
	assert ($plain != $recovered) "the external edit should change the recovered package"
	let watched = tg checkin --no-tokens $package --watch | referent node
	assert ($watched == $plain) "the recovered watch should reflect the external edit"
}
