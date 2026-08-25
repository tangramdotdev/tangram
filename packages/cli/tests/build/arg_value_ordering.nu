use ../../test.nu *

# A build preserves command-line order when `--arg-value` and `--arg-string` are interleaved, and accepts repeated `--arg-value`.

let server = server spawn

let path = artifact {
	tangram.ts: 'export default function (...a: Array<unknown>) { return a.map((v) => JSON.stringify(v)).join(","); }'
}

snapshot (tg build $path --arg-value '1' --arg-string 'S') '"1,\"S\""'
snapshot (tg build $path --arg-string 'S' --arg-value '1') '"\"S\",1"'
snapshot (tg build $path --arg-value '1' --arg-value '2') '"1,2"'
