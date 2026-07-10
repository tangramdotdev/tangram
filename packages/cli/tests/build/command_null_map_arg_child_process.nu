use ../../test.nu *

# A map arg with a null entry must round-trip into a child process with the key preserved, distinct from an absent key. Existence is checked with `!== undefined`, not the `in` operator.

let server = spawn

let path = artifact {
	tangram.ts: r#'
		export const inner = (arg: { a?: string | null }) => {
			return arg.a !== undefined ? "present" : "absent";
		};
		export default async () => {
			let withNull = await tg.build(inner, { a: null });
			tg.assert(withNull === "present", `expected the key to be preserved, got ${withNull}`);
			let withEmpty = await tg.build(inner, {});
			tg.assert(withEmpty === "absent", `expected no key, got ${withEmpty}`);
			return "ok";
		};
	'#
}

success (tg build $path | complete) "a map arg with a null entry round-trips with the key preserved, distinct from an empty map"
