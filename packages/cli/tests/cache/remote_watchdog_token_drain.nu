use ../../test.nu *

# Test that staggered callers do not receive TOCTOU cancellation errors
# when the watchdog cancels a shared dependency resolved via remote.
#
# Only the shared build process is pushed to the remote. The wrapper and
# outer modules execute locally on the fresh server. Each wrapper's
# process_task calls tg.build(shared), producing the same command hash
# as the pushed process. Staggered arrival (from process semaphore
# batching) means some callers arrive during the watchdog's cancellation
# window, receiving the cancellation error instead of the remote result.

let remote = spawn -n remote -u "http://localhost:8473" -c { tokio_single_threaded: false }
let primary = spawn -n primary -c { tokio_single_threaded: false }
tg remote put default $remote.url

# Shared build that takes time.
let shared = artifact {
	tangram.ts: '
		export default async () => {
			await tg.sleep(5);
			return tg.directory({ "result.txt": tg.file("shared result") });
		};
	'
}

# Build the shared module on the primary and push ONLY that process.
let shared_process = tg build -d $shared | str trim
tg wait $shared_process
tg index
tg push --eager --outputs --recursive $shared_process

# Wrapper: builds the shared dependency.
let wrapper = mktemp -d
let wrapper_ts = [
	$'import shared from "shared" with { local: "($shared)" };'
	'export default async (name) => {'
	'	const dir = await tg.build(shared).then(tg.Directory.expect);'
	'	return tg.directory({ [name]: dir });'
	'};'
] | str join "\n"
$wrapper_ts | save ($wrapper | path join "tangram.ts")

# 20 concurrent callers — enough for batching without permit starvation.
let outer = mktemp -d
let count = 20
let builds = 0..<$count | each { |i|
	'tg.build(wrap, "item' + ($i | into string) + '").then(tg.Directory.expect)'
} | str join ", "
let outer_ts = [
	$'import wrap from "wrap" with { local: "($wrapper)" };'
	''
	'export default async () => {'
	('	const results = await Promise.all([' + $builds + ']);')
	'	return tg.directory(Object.fromEntries(results.map((d, i) => [`r${i}`, d])));'
	'};'
] | str join "\n"
$outer_ts | save ($outer | path join "tangram.ts")

# Fresh server with concurrency enabled.
let fresh = spawn -n fresh -c { tokio_single_threaded: false, advanced: { single_process: false }, v8_thread_pool_size: 8 }
tg remote put default $remote.url

let fresh_result = do { tg build $outer } | complete

# The build should succeed — any cancellation error reaching a caller is the bug.
success $fresh_result
