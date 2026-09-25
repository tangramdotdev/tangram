use ../lib/test.nu *

# The position and length flags window a process's children list.

let server = server spawn --config { advanced: { checkpoints: true } }

let path = artifact {
	tangram.ts: '
		export function a() { return "a"; }
		export function b() { return "b"; }
		export function c() { return "c"; }
		export default async function () {
			await tg.build(a).named("a");
			await tg.build(a).named("duplicate");
			await tg.build(b).named("b");
			await tg.build(c).named("c");
			return "done";
		}
	'
}
let watch = tg checkpoint watch runner.process.control.retention.finished | from json | get watch
let build = tg build --detach --verbose $path | from json
tg wait $build.process

let all = tg process children $build.process | from json
assert equal ($all | child_names) [a b c] "the children should be in spawn order"

let chunked = tg process children --size 1 $build.process | from json
assert equal ($chunked | child_names) [a b c] "chunking should preserve spawn order"

let first = tg process children --length 1 $build.process | from json
assert equal ($first | child_names) [a] "the length flag should limit the list"

let rest = tg process children --position 1 $build.process | from json
assert equal ($rest | child_names) [b c] "the position flag should skip the beginning of the list"

let middle = tg process children --position 1 --length 1 $build.process | from json
assert equal ($middle | child_names) [b] "the position and length flags should combine"

let tail = tg process children --position=end.-2 --size 1 $build.process | from json
assert equal ($tail | child_names) [b c] "end-relative positions should work across chunks"

# End-relative reads report absolute positions for every source.
tg wait --source=index $build.process | ignore
let process = $build.process | split row '?' | first
let socket = $server.url | str replace 'http+unix://' '' | url decode
for source in [auto runner index] {
	let output = http get --raw --max-time 10sec --unix-socket $socket $'http://localhost/processes/($process)/children?source=($source)&position=end.-2&size=1'
	let chunks = $output | lines | where { $in starts-with 'data: ' } | each { str substring 6.. | from json }
	assert equal ($chunks | get position) [1 2]
}

tg checkpoint unwatch runner.process.control.retention.finished $watch

def child_names [] {
	each { |child|
		$child.process
		| url parse --base 'tg:///'
		| get params
		| where key == name
		| first
		| get value
	}
}
