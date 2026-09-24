use ../../lib/test.nu *

# Shared descendants are visited a bounded number of times and their tokens are normalized together.
let server = server spawn
let path = artifact {
	tangram.ts: '
		export default function () {
			function measure(count: number) {
				const encode = (value) => tg.encoding.base64.encode(tg.encoding.utf8.encode(JSON.stringify(value)));
				let root = tg.Directory.withObject({ entries: {} });
				for (let i = 0; i < count; i++) {
					root = tg.Directory.withObject({ entries: { a: root, b: root } });
					const token = `0.${encode({ expires_at: 120, permissions: ["object_node"], resource: root.id })}.${encode({ algorithm: "ed25519", key: "test" })}.`;
					root.state.tokens = { local: { authorization: [token, `sync-${i}`] } };
				}
				const children = tg.Object.Object.children;
				const normalize = tg.Tokens.normalize;
				let visits = 0;
				let normalizations = 0;
				let tokenInputs = 0;
				tg.Object.Object.children = (object) => { visits++; return children(object); };
				tg.Tokens.normalize = (tokens, resource) => {
					normalizations++;
					for (const entry of Object.values(tokens)) tokenInputs += (entry.authorization?.length ?? 0);
					return normalize(tokens, resource);
				};
				try {
					const tokens = root.state.collectTokens();
					tg.assert(tokens.local.authorization.length === count * 2);
				} finally {
					tg.Object.Object.children = children;
					tg.Tokens.normalize = normalize;
				}
				return { normalizations, tokenInputs, visits };
			}
			return [measure(64), measure(128)];
		}
	'
}
let output = tg build $path | complete
success $output
let counts = $output.stdout | from json
for sample in ($counts | enumerate) {
	let count = if $sample.index == 0 { 64 } else { 128 }
	assert equal $sample.item.normalizations 1
	assert equal $sample.item.tokenInputs ($count * 2)
	assert ($sample.item.visits <= 2 * ($count + 1)) 'collection should visit each shared object at most once per traversal'
}
