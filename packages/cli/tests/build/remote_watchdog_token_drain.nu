use ../../test.nu *

# Test that staggered callers do not receive TOCTOU cancellation errors
# when the watchdog cancels a shared dependency resolved via remote.
#
# Only the shared build process is pushed to the remote. Wrapper builds
# execute locally on the fresh server. Each wrapper's process_task calls
# tg.build(shared), which produces the same command hash as the pushed
# process. Staggered arrival (from process semaphore batching) creates
# the conditions for the TOCTOU: the watchdog cancels the shared process
# while a later caller has already found it as Started.

let remote = spawn -n remote
let primary = spawn -n primary
tg remote put default $remote.url

# Shared build that takes time.
let shared = artifact {
	tangram.ts: '
		export default async () => {
			await tg.sleep(3);
			return tg.directory({ "result.txt": tg.file("shared result") });
		};
	'
}

# Build the shared module on the primary and push only that process.
let shared_process = tg build -d $shared | str trim
tg wait $shared_process
tg index
tg push --eager --outputs --recursive $shared_process

# Wrapper: builds the shared dependency.
let wrapper = mktemp -d
let wrapper_ts = [
	$'import shared from "shared" with { source: "($shared)" };'
	'export default async (name) => {'
	'	const dir = await tg.build(shared).then(tg.Directory.expect);'
	'	return tg.directory({ [name]: dir });'
	'};'
] | str join "\n"
$wrapper_ts | save ($wrapper | path join "tangram.ts")

# Run 100 concurrent builds that use the wrapper.
let count = 100
let builds = 0..<$count | each { |i|
	'tg.build(wrap, "item' + ($i | into string) + '").then(tg.Directory.expect)'
} | str join ", "
let outer = mktemp -d
let outer_ts = [
	$'import wrap from "wrap" with { source: "($wrapper)" };'
	''
	'export default async () => {'
	('	const results = await Promise.all([' + $builds + ']);')
	'	return tg.directory(Object.fromEntries(results.map((d, i) => [`r${i}`, d])));'
	'};'
] | str join "\n"
$outer_ts | save ($outer | path join "tangram.ts")

# Fresh server with concurrency enabled.
let fresh = spawn -n fresh
tg remote put default $remote.url

let fresh_result = do { tg build $outer } | complete

# The build should succeed — any cancellation error reaching a caller is the bug.
success $fresh_result
