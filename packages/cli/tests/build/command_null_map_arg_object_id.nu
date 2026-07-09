use ../../test.nu *

# A command argument map with a null entry must round-trip through the object
# store. The id is computed over the serde/v8 bridge, which keeps the null-valued
# key, while the object is sent as JSON; null maps directly to the wire with no
# translation, so the key survives JSON.stringify. A divergence throws "invalid
# object id".

let server = spawn

let path = artifact {
	tangram.ts: r#'
		export function f() {
			return "ok";
		}
		export default async function () {
			// Construct a command whose argument is a map with a null entry.
			let command = await tg.command(f, { a: null });

			// Store it. A mismatch between the local id and the wire bytes fails
			// here with "invalid object id".
			let id = await command.store();

			// Load it back and assert the null entry survived.
			let args = await tg.Command.withId(id).args;
			tg.assert(args.length === 1, "the argument was dropped");
			let arg = args[0];
			tg.assert(
				typeof arg === "object" && arg !== null && arg.a === null,
				"the null map entry was dropped",
			);

			return "ok";
		}
	'#
}

success (tg build $path | complete) "a null map entry must round-trip through the object store"
