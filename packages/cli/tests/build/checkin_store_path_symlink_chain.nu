use ../lib/test.nu *

# Symlink targets retain proofs through chains, duplicate inputs, and reused physical checkouts.

let server = server spawn --config {
	authorization: { final: false, initial: false }
	vfs: false
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			const directory = await tg.directory({ nested: {} });
			const symlink = await tg.symlink({ artifact: directory, path: "nested" });
			const chain = await tg.symlink({ artifact: symlink });
			await chain.store();
			const bare = tg.Symlink.withId(chain.id);
			tg.assert(tg.Authorization.Tokens.isEmpty(bare.state.tokens));
			for (const env of [
				{ A_BARE: bare, INPUT: chain },
				{ INPUT: chain, Z_BARE: bare },
			]) {
				const output = await tg.build`
					tg checkin "\${INPUT%/*}/${directory.id}" > ${tg.output}
				`.env(env).then(tg.File.expect);
				tg.assert((await output.text).trim().split("?")[0] === directory.id);
			}
		}
		export async function reuse() {
			const directory = await tg.directory({});
			return tg.command({
				args: ["-ec", `tg checkin "\${INPUT%/*}/${directory.id}"`],
				env: { INPUT: tg.symlink({ artifact: directory }) },
				executable: "/bin/sh",
				host: tg.host.current,
			});
		}
		export async function cycle() {
			const graph = await tg.graph({ nodes: [
				{ kind: "symlink", artifact: 1 },
				{ kind: "symlink", artifact: 0 },
			] });
			return tg.command({
				args: ["-ec", "tg --version"],
				env: { CYCLE: tg.symlink({ graph, index: 0, kind: "symlink" }) },
				executable: "/bin/sh",
				host: tg.host.current,
			});
		}
	'
}

let output = tg build $path | complete
success $output "the process should check in symlink targets without searching or looping"

# Reuse the checkout in a new sandbox that cannot inherit the first sandbox's target proof.
let command = tg build $'($path)#reuse' | str trim
for _ in 1..2 {
	let sandbox = tg sandbox create --no-network | str trim
	let output = tg run $'--sandbox=($sandbox)' $command | complete
	success $output "a reused checkout must retain the target proof in each sandbox"
	tg sandbox destroy $sandbox
}

# Proof traversal must terminate so checkout can report the filesystem symlink loop.
let command = tg build $'($path)#cycle' | str trim
let output = timeout 30s tg run --sandbox $command | complete
failure $output
assert ($output.stderr =~ '(?i)too many levels of symbolic links')
