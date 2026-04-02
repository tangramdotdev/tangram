use ../../test.nu *

# Test that the watchdog does not cancel a process that is actively
# executing when all callers' remote lookups have resolved.

let remote = spawn -n remote -u "http://localhost:8473" -c { tokio_single_threaded: false }
let local = spawn -n local -c { tokio_single_threaded: false }
tg remote put default $remote.url

# Shared build that takes 5 seconds to execute locally.
let shared = artifact {
	tangram.ts: '
		export default async () => {
			await tg.sleep(5);
			return tg.directory({ "result.txt": tg.file("shared result") });
		};
	'
}

# Wrapper: calls tg.build(shared).
let wrapper = mktemp -d
let wrapper_ts = [
	$'import shared from "shared" with { local: "($shared)" };'
	'export default async (name) => {'
	'	const dir = await tg.build(shared).then(tg.Directory.expect);'
	'	return tg.directory({ [name]: dir });'
	'};'
] | str join "\n"
$wrapper_ts | save ($wrapper | path join "tangram.ts")

# 200 concurrent callers to maximize token accumulation.
let outer = mktemp -d
let count = 200
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

let id = tg build $outer
tg push --eager --outputs --recursive $id

# Fresh: multi-threaded, single_process disabled, fast watchdog.
let fresh = spawn -n fresh -c { tokio_single_threaded: false, advanced: { single_process: false } }
tg remote put default $remote.url

let fresh_id = tg build $outer

# Check for watchdog cancellations.
let cancelled_count = (sqlite3 ($fresh.directory | path join "database") "SELECT count(*) FROM processes WHERE error_code = 'cancellation'" | str trim | into int)
if $cancelled_count > 0 {
	print -e $"FAIL: ($cancelled_count) processes cancelled by watchdog"
}
assert ($cancelled_count == 0) "watchdog cancelled executing processes"

snapshot $fresh_id $id
