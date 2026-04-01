use ../../test.nu *

# Test that concurrent builds of the same shared dependency correctly
# use remote cache hits.

let remote = spawn -n remote -u "http://localhost:8473" -c { tokio_single_threaded: false }
let local = spawn -n local -c { tokio_single_threaded: false }
tg remote put default $remote.url

let shared = artifact {
	tangram.ts: '
		export default async () => {
			await tg.sleep(0.5);
			return tg.directory({ "result.txt": tg.file("shared") });
		};
	'
}

let middle = mktemp -d
let middle_ts = [
	$'import shared from "shared" with { local: "($shared)" };'
	''
	'export default async (name) => {'
	'	const dir = await tg.build(shared).then(tg.Directory.expect);'
	'	return tg.directory({ [name]: dir });'
	'};'
] | str join "\n"
$middle_ts | save ($middle | path join "tangram.ts")

let outer = mktemp -d
let count = 50
let builds = 0..<$count | each { |i|
	'tg.build(middle, "item' + ($i | into string) + '").then(tg.Directory.expect)'
} | str join ", "
let tangram_ts = [
	$'import middle from "middle" with { local: "($middle)" };'
	''
	'export default async () => {'
	('	const results = await Promise.all([' + $builds + ']);')
	'	return tg.directory(Object.fromEntries(results.map((d, i) => [`r${i}`, d])));'
	'};'
] | str join "\n"
$tangram_ts | save ($outer | path join "tangram.ts")

let process_id = tg build -d $outer | str trim
tg wait $process_id
tg index

tg push --eager --outputs --recursive $process_id

let fresh = spawn -n fresh -c { tokio_single_threaded: false }
tg remote put default $remote.url

let output = tg build $outer
snapshot $output
