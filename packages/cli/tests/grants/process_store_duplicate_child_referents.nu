use ../../test.nu *

# Repeated children retain any available subtree proof, independent of referent order.
let server = server spawn
let path = artifact {
	tangram.ts: '
		export const source = async (seed: string) => {
			const child = await tg.file(seed);
			const other = await tg.file(`other ${seed}`);
			const parent = await tg.directory({ child });
			const unrelated = await tg.directory({ other });
			await tg.Value.store([parent, unrelated]);
			return {
				child: child.id,
				exact: child.state.tokens.local,
				inherited: parent.state.tokens.local,
				other: other.id,
				unrelated: unrelated.state.tokens.local,
			};
		};

		export default async (kind: string, order: string, json: string) => {
			const input = JSON.parse(json);
			const referent = (token: string, id = input.child) =>
				tg.File.withReferent({ node: id, options: { tokens: { local: token } } });
			const bare = tg.File.withId(input.child);
			const exact = referent(input.exact);
			const inherited = referent(input.inherited);
			const unrelated = referent(input.unrelated);
			const fresh = await tg.file(`fresh ${order}`);
			const pairs = {
				alternatives: [unrelated, inherited],
				bare: [bare, bare],
				batch: [fresh, fresh],
				exact: [bare, exact],
				inherited: [bare, inherited],
				partial: [unrelated, inherited],
				unrelated: [unrelated, unrelated],
			};
			const [a, b] = order === "forward" ? pairs[kind] : pairs[kind].reverse();
			const output = await tg.directory({
				a, b,
				...(kind === "partial" ? { c: referent(input.inherited, input.other) } : {}),
			});
			await output.store();
			return { token: output.state.tokens.local };
		};
	'
}

for case in [
	[kind permission];
	[alternatives object_subtree]
	[bare object_node]
	[batch object_subtree]
	[exact object_subtree]
	[inherited object_subtree]
	[partial object_node]
	[unrelated object_node]
] {
	for order in [forward reverse] {
		# Pass tokens as strings between independent processes so grants cannot substitute for the token proofs.
		let input = tg build $'($path)#source' -a $'($case.kind)-($order)' | str trim
		let output = tg build $path -a $case.kind -a $order -a $input | complete
		success $output $'storing ($case.kind) children in ($order) order should succeed'
		let permissions = $output.stdout | from json | get token | split row '.' | get 1 | decode base64 | decode utf-8 | from json | get permissions
		assert equal $permissions [$case.permission] $'unexpected permissions for ($case.kind) children in ($order) order'
	}
}
