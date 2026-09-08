use ../../test.nu *

# Two directory entries can name the same child but carry different authorization tokens.
# The directory should receive subtree permission if every child has at least one valid proof.
let server = server spawn
let path = artifact {
	tangram.ts: '
		export const source = async (seed: string) => {
			// Create two separate trees: parent contains child, and unrelated contains other.
			const child = await tg.file(seed);
			const other = await tg.file(`other ${seed}`);
			const parent = await tg.directory({ child });
			const unrelated = await tg.directory({ other });
			await tg.Value.store([parent, unrelated]);

			// The exact token names child; the inherited token names its parent.
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
			const withToken = (id: tg.File.Id, token: string) =>
				tg.File.withReferent({ node: id, options: { tokens: { local: token } } });

			// These referents name the same stored file and differ only in their tokens.
			const bare = tg.File.withId(input.child);
			const exact = withToken(input.child, input.exact);
			const inherited = withToken(input.child, input.inherited);
			const unrelated = withToken(input.child, input.unrelated);

			// This file is stored in the same batch as the output, so it needs no existing token.
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
			const pair = pairs[kind];
			if (order === "reverse") {
				pair.reverse();
			}
			const [a, b] = pair;
			const entries: Record<string, tg.Artifact> = { a, b };
			if (kind === "partial") {
				// The proof for child does not authorize the separate file named other.
				entries.c = withToken(input.other, input.inherited);
			}

			const output = await tg.directory(entries);
			await output.store();
			return { token: output.state.tokens.local };
		};
	'
}

for case in [
	[kind permission];
	# One of two different ancestor tokens proves access.
	[alternatives object_subtree]
	# Repeating a bare ID provides no proof.
	[bare object_node]
	# A new child can be authorized within the batch itself.
	[batch object_subtree]
	# Either an exact token or an ancestor token can supply the missing proof.
	[exact object_subtree]
	[inherited object_subtree]
	# A proof for one child cannot compensate for another child without a proof.
	[partial object_node]
	# Repeating a token for the wrong tree provides no proof.
	[unrelated object_node]
] {
	for order in [forward reverse] {
		# Pass tokens as strings between independent processes so grants cannot substitute for the token proofs.
		# Give each case distinct content so earlier cases cannot authorize later ones.
		let input = tg build $'($path)#source' -a $'($case.kind)-($order)' | str trim
		let output = tg build $path -a $case.kind -a $order -a $input | complete
		success $output $'storing ($case.kind) children in ($order) order should succeed'

		# The second token component is its base64-encoded JSON body.
		let token = $output.stdout | from json | get token
		let body = $token | split row '.' | get 1 | decode base64 | decode utf-8 | from json
		let permissions = $body.permissions
		assert equal $permissions [$case.permission] $'unexpected permissions for ($case.kind) children in ($order) order'
	}
}
