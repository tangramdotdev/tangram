use ../../test.nu *

# Null map entries survive the JSON network boundary at every position (top-level,
# nested, in arrays, and alongside artifacts), round-tripping as null and staying
# distinct from an absent key. Null maps directly to the wire with no translation.

let server = spawn

let path = artifact {
	tangram.ts: r#'
		export function f() {
			return "ok";
		}
		export default async function () {
			let file = await tg.file("hello");
			let command = await tg.command(f, {
				a: null,
				file,
				nested: { b: null },
				list: [null, "x", file],
			});
			let id = await command.store();
			let arg = (await tg.Command.withId(id).args)[0];
			tg.assert(
				typeof arg === "object" && arg !== null && arg.a === null,
				"the top-level null entry did not round-trip",
			);
			tg.assert(
				typeof arg === "object" &&
					arg !== null &&
					typeof arg.nested === "object" &&
					arg.nested !== null &&
					arg.nested.b === null,
				"the nested null entry did not round-trip",
			);
			tg.assert(
				typeof arg === "object" &&
					arg !== null &&
					arg.file instanceof tg.File,
				"the artifact reference alongside null entries was lost",
			);
			tg.assert(
				typeof arg === "object" &&
					arg !== null &&
					Array.isArray(arg.list) &&
					arg.list.length === 3 &&
					arg.list[0] === null &&
					arg.list[1] === "x" &&
					arg.list[2] instanceof tg.File,
				"the array mixing null, a string, and an artifact did not round-trip",
			);

			// A null entry stays distinct from an absent key.
			let empty = await tg.command(f, {});
			tg.assert(
				(await empty.store()) !== id,
				"a null entry must be distinct from an absent key",
			);

			return "ok";
		}
	'#
}

success (tg build $path | complete) "null entries and artifacts survive the JSON boundary and stay distinct from absent keys"
