use ../../test.nu *

# Two distinct parent processes that concurrently build the same child command
# must both observe the deduplicated child, rather than one of them failing to
# wait for it.
#
# Regression test: concurrently building a shared command from two separate
# processes races the spawn deduplication path, so one parent's wait on the
# deduplicated process returns "failed to find the process".

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await Promise.all([
				tg.build(parent, 1).named("a"),
				tg.build(parent, 2).named("b"),
			]);
		}
		export async function parent(n: number) {
			return await tg.build(shared).named("shared");
		}
		export async function shared() {
			await tg.sleep(0.1);
			return 42;
		}
	'
}

let output = tg build $path | from json
assert equal $output [42, 42]
