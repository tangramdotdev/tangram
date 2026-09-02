use ../../../test.nu *

# Concurrent stores share one promise, update every state, and batch each ID once.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			const postObjectBatch = tg.client.postObjectBatch;
			let batches = 0;
			let childrenFirst = true;
			let independent = false;
			let objects = 0;
			let unique = true;
			let updated = false;
			try {
				tg.client.postObjectBatch = async (arg) => {
					batches += 1;
					objects += arg.objects.length;
					const ids = arg.objects.map((object) => object.id);
					unique &&= new Set(ids).size === ids.length;
					const seen = new Set<tg.Object.Id>();
					for (const object of arg.objects) {
						for (const child of object.children ?? []) {
							childrenFirst &&= seen.has(child.node);
						}
						seen.add(object.id);
					}
					await tg.sleep(0.05);

					return {
						objects: arg.objects.map(({ id }) => ({
							node: id,
							options: {
								location: { name: "test" },
								tokens: { test: "token" },
							},
						})),
					};
				};

				const leafA = tg.Blob.withObject({ bytes: new Uint8Array([1]) });
				const leafB = tg.Blob.withObject({ bytes: new Uint8Array([1]) });
				const left = tg.Blob.withObject({
					children: [{ blob: leafA, length: 1 }],
				});
				const right = tg.Blob.withObject({
					children: [{ blob: leafB, length: 1 }],
				});
				const root = tg.Blob.withObject({
					children: [
						{ blob: left, length: 1 },
						{ blob: right, length: 1 },
					],
				});
				await Promise.all([
					root.store(),
					root.store(),
					tg.Value.store([root, left, right, leafA, leafB]),
				]);
				updated = [root, left, right, leafA, leafB].every((object) => {
					return (
						object.state.stored &&
						JSON.stringify(object.state.location) === '{"name":"test"}' &&
						object.state.tokens.test === "token"
					);
				});
				leafA.state.inheritTokens({ independent: "independent" });
				independent = leafB.state.tokens.independent === undefined;

				return {
					batches,
					childrenFirst,
					independent,
					objects,
					unique,
					updated,
				};
			} finally {
				tg.client.postObjectBatch = postObjectBatch;
			}
		}
	'
}

let output = tg build $path
snapshot $output '{"batches":1,"childrenFirst":true,"independent":true,"objects":3,"unique":true,"updated":true}'
