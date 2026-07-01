use ../../test.nu *

# Undefined map entries survive the JSON network boundary at every position (top-
# level, nested, in arrays, and alongside artifacts), round-tripping as undefined
# and staying distinct from an absent key.

let server = spawn

let path = artifact {
	tangram.ts: r#'
		export function f() {
			return "ok";
		}
		export default async function () {
			let file = await tg.file("hello");
			let command = await tg.command(f, {
				a: undefined,
				file,
				nested: { b: undefined },
				list: [undefined, "x", file],
			});
			let id = await command.store();
			let arg = (await tg.Command.withId(id).args)[0];
			tg.assert(
				typeof arg === "object" && arg !== null && "a" in arg && arg.a === undefined,
				"the top-level undefined entry did not round-trip",
			);
			tg.assert(
				typeof arg === "object" &&
					arg !== null &&
					"nested" in arg &&
					typeof arg.nested === "object" &&
					arg.nested !== null &&
					"b" in arg.nested &&
					arg.nested.b === undefined,
				"the nested undefined entry did not round-trip",
			);
			tg.assert(
				typeof arg === "object" &&
					arg !== null &&
					"file" in arg &&
					arg.file instanceof tg.File,
				"the artifact reference alongside undefined entries was lost",
			);
			tg.assert(
				typeof arg === "object" &&
					arg !== null &&
					"list" in arg &&
					Array.isArray(arg.list) &&
					arg.list.length === 3 &&
					arg.list[0] === undefined &&
					arg.list[1] === "x" &&
					arg.list[2] instanceof tg.File,
				"the array mixing undefined, a string, and an artifact did not round-trip",
			);

			// An absent key stays distinct from an undefined entry.
			let empty = await tg.command(f, {});
			tg.assert(
				(await empty.store()) !== id,
				"an undefined entry must be distinct from an absent key",
			);

			return "ok";
		}
	'#
}

success (tg build $path | complete) "undefined entries and artifacts survive the JSON boundary and stay distinct from absent keys"
