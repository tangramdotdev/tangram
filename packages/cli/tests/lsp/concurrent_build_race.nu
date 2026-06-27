use ../../test.nu *
use ../lib/lsp.nu

# Builds running concurrently with the LSP TypeScript service do not crash the
# server when both V8 isolates run at the same time.
#
# Regression test for 596e23f2 (#757).

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return 42; }'
}

let file_uri = lsp uri ($path | path join 'tangram.ts')

# Generate many hover requests to keep TypeScript V8 isolate busy.
let hovers = 1..500 | each { |i|
	lsp hover ($i + 10) $file_uri 0 ($i mod 30)
}

# Start LSP with hover requests in background.
let lsp_job = lsp run_background ([
	(lsp initialize 1)
	(lsp initialized)
	(lsp did_open $file_uri "export default async function () { return 42; }\n")
] | append $hovers)

# There is no observable condition for the hover requests being in flight, so give the LSP a moment to start processing them before building concurrently.
sleep 100ms

# Run multiple builds while LSP is processing to increase chance of hitting race.
for i in 1..3 {
	let output = tg build $path | complete
	if $output.exit_code != 0 {
		error make { msg: $"build ($i) failed - possible race condition crash" }
	}
	let health = tg health | complete
	if $health.exit_code != 0 {
		error make { msg: $"server crashed after build ($i)" }
	}
}

try {
	lsp wait_background $lsp_job --timeout 5sec
} catch {
	job kill $lsp_job
}
