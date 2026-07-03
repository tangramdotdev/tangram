use ../../test.nu *

# A map arg with an undefined entry must round-trip into a child process with the key preserved, distinct from an absent key.

let server = spawn

let path = artifact {
	tangram.ts: r#'
		export const inner = (arg: { a?: string }) => {
			return "a" in arg ? "present" : "absent";
		};
		export default async () => {
			let withUndefined = await tg.build(inner, { a: undefined });
			tg.assert(withUndefined === "present", `expected the key to be preserved, got ${withUndefined}`);
			let withEmpty = await tg.build(inner, {});
			tg.assert(withEmpty === "absent", `expected no key, got ${withEmpty}`);
			return "ok";
		};
	'#
}

success (tg build $path | complete) "a map arg with an undefined entry round-trips with the key preserved, distinct from an empty map"
