use ../../../util/test.nu *

# A module whose static imports share a dependency and form a cycle evaluates once with all of its imports available.
let local = server spawn

let path = artifact {
	tangram.ts: '
		import { left } from "./left.tg.ts";
		import { right } from "./right.tg.ts";
		export default () => [left(), right()];
	'
	left.tg.ts: '
		import { shared } from "./shared.tg.ts";
		import { right } from "./right.tg.ts";
		export const left = () => `left:${shared}`;
		export const viaRight = () => right();
	'
	right.tg.ts: '
		import { shared } from "./shared.tg.ts";
		import { left } from "./left.tg.ts";
		export const right = () => `right:${shared}`;
		export const viaLeft = () => left();
	'
	shared.tg.ts: '
		export const shared = "shared";
	'
}

let output = tg build $path | complete
success $output
assert equal ($output.stdout | from json) ['left:shared' 'right:shared']
