use ../../lib/test.nu *

let server = server spawn --config {
	authorization: { final: false, initial: false }
}
let path = artifact {
	tangram.ts: '
		export default async function () {
			for (const reverse of [false, true]) {
				const a = await tg.file(`left ${reverse}`);
				const b = await tg.file(`right ${reverse}`);
				await tg.Value.store([a, b]);
				const left = tg.Directory.withObject({ entries: { a, b: tg.File.withId(b.id) } });
				const right = tg.Directory.withObject({ entries: { a: tg.File.withId(a.id), b } });
				tg.assert(left.id === right.id);
				await tg.Value.store(reverse ? [right, left] : [left, right]);
				for (const directory of [left, right]) {
					tg.assert(directory.state.stored);
					tg.assert(directory.state.tokens.local.some((token) => tg.Authorization.Token.grantsObjectSubtree(token, directory.id)));
				}
				await tg.build`true ${left}`;
			}
			return true;
		}
	'
}
assert equal (tg build $path | from json) true
