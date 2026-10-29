use ../../util/test.nu *
use ../../util/module.nu *

# Resolving Edge::Object modules returns tokens for the resolved file IDs, so a user who holds only the command token can resolve each subsequent import.

let local = server spawn --config {
	authentication: { users: { providers: { insecure: true } } }
}

let alice = tg login --verbose --name alice | from json
let bob = tg login --verbose --name bob | from json

# The command makes A a child and provides the sole token Bob uses for the first resolution.
let path = artifact {
	tangram.ts: '
		export default async function () {
			const c = await tg.file({ contents: "export default 3;", module: "typescript" });
			const bModule = await tg.file({
				contents: `import "./c"; export default 2;`,
				dependencies: { "./c": c },
				module: "typescript",
			});
			const bChildDirectory = await tg.directory({ "tangram.ts": bModule });
			const bDirectory = await tg.directory({
				children: [{ directory: bChildDirectory, count: 1, last: "tangram.ts" }],
			});
			const a = await tg.file({
				contents: `import "./b"; export default 1;`,
				dependencies: { "./b": bDirectory },
				module: "typescript",
			});
			const module = new tg.Module({
				kind: "typescript",
				referent: { node: a, options: {} },
			});
			const command = await tg.command({
				args: [tg.Command.Value.value(module)],
				executable: "tg",
				host: tg.host.current,
			});
			await command.store();
			a.state.tokens = {};
			module.referent.options ??= {};
			module.referent.options.tokens = command.state.tokens;
			return {
				a: a.id,
				b: bModule.id,
				bChildDirectory: bChildDirectory.id,
				bDirectory: bDirectory.id,
				c: c.id,
				module: tg.Module.toData(module),
			};
		}
	'
}

let case = tg --token $alice.token build $path | from json
tg --token $alice.token index
assert equal $case.module.referent.node $case.a

let socket = $local.url | str replace 'http+unix://' '' | url decode

# The first resolution returns a token through which Bob can read B, which Bob cannot read by its ID alone.
failure (tg --token $bob.token get $case.b | complete) 'bob must not read the object by its ID alone'
let b = resolve-module $socket $bob.token $case.module './b'
success (read-with-module-tokens $bob.token $case.b $b.module) 'bob must read B through the returned token'

# Resolving B with the returned token returns a token through which Bob can read C.
let c = resolve-module $socket $bob.token $b.module './c'
success (read-with-module-tokens $bob.token $case.c $c.module) 'bob must read C through the returned token'
