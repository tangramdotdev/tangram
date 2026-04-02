use ../../test.nu *

# Test that the watchdog does not cancel a process with an active task
# when all callers' remote lookups drain its token_count to 0.

let remote = spawn -n remote -u "http://localhost:8473" -c { tokio_single_threaded: false }
let local = spawn -n local -c { tokio_single_threaded: false }
tg remote put default $remote.url

# Shared build that takes time. With single_process disabled, multiple
# wrappers execute concurrently and find this as Started, adding tokens.
let shared = artifact {
	tangram.ts: '
		export default async () => {
			await tg.sleep(3);
			return tg.directory({ "result.txt": tg.file("shared result") });
		};
	'
}

# Wrapper: each call independently builds the shared dependency.
let wrapper = mktemp -d
let wrapper_ts = [
	$'import shared from "shared" with { local: "($shared)" };'
	'export default async (name) => {'
	'	const dir = await tg.build(shared).then(tg.Directory.expect);'
	'	return tg.directory({ [name]: dir });'
	'};'
] | str join "\n"
$wrapper_ts | save ($wrapper | path join "tangram.ts")

# 20 concurrent callers.
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

let process_id = tg build -d $outer | str trim
tg wait $process_id
tg index
tg push --eager --outputs --recursive $process_id
let id = tg output $process_id | str trim

# Fresh: multi-threaded, single_process disabled, v8 pool size > 1 so
# multiple wrapper JS evaluations run truly concurrently.
let fresh = spawn -n fresh -c { tokio_single_threaded: false, advanced: { single_process: false }, v8_thread_pool_size: 8 }
tg remote put default $remote.url

let fresh_result = do { tg build $outer } | complete

# Check for watchdog cancellations regardless of build result.
let cancelled_count = (sqlite3 ($fresh.directory | path join "database") "SELECT count(*) FROM processes WHERE error_code = 'cancellation'" | str trim | into int)
print -e $"Cancelled processes: ($cancelled_count)"
assert ($cancelled_count == 0) $"watchdog cancelled ($cancelled_count) executing processes"

success $fresh_result
snapshot ($fresh_result.stdout | str trim) $id
