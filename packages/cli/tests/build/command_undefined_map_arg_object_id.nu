use ../../test.nu *

# A command argument map with an undefined entry must round-trip through the
# object store. The id is computed over the serde/v8 bridge, which keeps the
# undefined-valued key, while the object is sent as JSON; undefined is converted
# to null at the JSON boundary so the key survives JSON.stringify. A divergence
# throws "invalid object id".

let server = spawn

let path = artifact {
	tangram.ts: r#'
		export function f() {
			return "ok";
		}
		export default async function () {
			// Construct a command whose argument is a map with an undefined entry.
			let command = await tg.command(f, { a: undefined });

			// Store it. A mismatch between the local id and the wire bytes fails
			// here with "invalid object id".
			let id = await command.store();

			// Load it back and assert the undefined entry survived.
			let args = await tg.Command.withId(id).args;
			tg.assert(args.length === 1, "the argument was dropped");
			let arg = args[0];
			tg.assert(
				typeof arg === "object" && arg !== null && "a" in arg,
				"the undefined map entry was dropped",
			);

			return "ok";
		}
	'#
}

success (tg build $path | complete) "an undefined map entry must round-trip through the object store"
