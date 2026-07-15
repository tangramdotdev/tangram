use ../../test.nu *

# The position and length flags window a process's children list.

let server = spawn

let path = artifact {
	tangram.ts: '
		export function a() { return "a"; }
		export function b() { return "b"; }
		export function c() { return "c"; }
		export default async function () {
			await tg.build(a).named("a");
			await tg.build(b).named("b");
			await tg.build(c).named("c");
			return "done";
		}
	'
}
let build = tg build --detach --verbose $path | from json
tg wait $build.process

let all = tg process children $build.process | from json
assert equal ($all | child_names) [a b c] "the children should be in spawn order"

let first = tg process children --length 1 $build.process | from json
assert equal ($first | child_names) [a] "the length flag should limit the list"

let rest = tg process children --position 1 $build.process | from json
assert equal ($rest | child_names) [b c] "the position flag should skip the beginning of the list"

let middle = tg process children --position 1 --length 1 $build.process | from json
assert equal ($middle | child_names) [b] "the position and length flags should combine"

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
