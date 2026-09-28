use ../lib/test.nu *

let server = server spawn --config {
	authorization: { final: false, initial: false }
}
let path = artifact {
	tangram.ts: '
		export default async function () {
			for (const reverse of [false, true]) {
				const a = await tg.file(`batch left ${reverse}`);
				const b = await tg.file(`batch right ${reverse}`);
				await tg.Value.store([a, b]);
				const directory = tg.Directory.withObject({ entries: { a, b } });
				const data = tg.Object.Data.withoutLocationAndTokens(tg.Object.Object.toData(directory.state.object!));
				const id = directory.id;
				const left = { children: [tg.Object.toReferent(a), { node: b.id }], data, id };
				const right = { children: [{ node: a.id }, tg.Object.toReferent(b)], data, id };
				const output = await tg.client.postObjectBatch({ objects: reverse ? [right, left] : [left, right] });
				tg.assert(output.objects.length === 2);
				for (const object of output.objects) {
					tg.assert(object.node === id);
					tg.assert(object.options.tokens.local.some((token) => tg.Authorization.Token.grantsObjectSubtree(token, id)));
				}
				const missing = { children: [tg.Object.toReferent(a), { node: b.id }], data, id };
				const denied = await tg.client.postObjectBatch({ objects: [missing, missing] });
				for (const object of denied.objects) {
					tg.assert(!object.options.tokens.local.some((token) => tg.Authorization.Token.grantsObjectSubtree(token, id)));
				}
			}
			return true;
		}
	'
}
assert equal (tg build $path | from json) true
