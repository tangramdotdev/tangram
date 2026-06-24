use ../../test.nu *

# A build whose module graph forms a diamond must authorize the shared
# dependency without hanging.

let server = spawn

let path = artifact {
	tangram.ts: '
		import a from "./a.tg.ts";
		import b from "./b.tg.ts";
		export default async function () {
			let x = await tg.build(a);
			let y = await tg.build(b);
			return `${x}${y}`;
		}
	'
	a.tg.ts: '
		import b from "./b.tg.ts";
		export default async function () {
			let y = await tg.build(b);
			return `a${y}`;
		}
	'
	b.tg.ts: '
		export default function () { return "b"; }
	'
}

let output = tg build $path | from json
assert equal $output "abb"
