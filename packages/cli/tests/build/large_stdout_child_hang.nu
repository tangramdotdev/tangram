use ../../test.nu *

# A log write failure fails the process instead of leaving the child blocked on stdout.

let server = server spawn --config {
	indexer: { log_compaction: false }
	store: {
		map_size: 20_971_520,
	}
}

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.build(noisy);
		}
		export function noisy() {
			for (let i = 0; i < 200000; i++) {
				console.log("padding line " + i + " ------------------------------------------------------------");
			}
			return tg.file("done");
		}
	'
}

let output = tg build $path | complete
assert equal $output.exit_code 1
