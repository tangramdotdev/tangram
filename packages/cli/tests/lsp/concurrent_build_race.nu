use ../../test.nu *

# Test for V8 race condition between LSP TypeScript service and build JS runtime.
# The crash occurs when both V8 isolates are running concurrently.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async () => 42;'
}

let file_uri = $"file://($path | path join 'tangram.ts' | path expand)"

def lsp_msg [content: string] {
	let len = $content | encode utf8 | bytes length
	$"Content-Length: ($len)\r\n\r\n($content)"
}

# Generate many hover requests to keep TypeScript V8 isolate busy.
let init = (lsp_msg '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"processId":null,"capabilities":{},"rootUri":null}}')
let initialized = (lsp_msg '{"jsonrpc":"2.0","method":"initialized","params":{}}')
let did_open = (lsp_msg $'{"jsonrpc":"2.0","method":"textDocument/didOpen","params":{"textDocument":{"uri":"($file_uri)","languageId":"tangram-typescript","version":1,"text":"export default async () => 42;\\n"}}}')
let hovers = 1..500 | each { |i|
	lsp_msg $'{"jsonrpc":"2.0","id":($i + 10),"method":"textDocument/hover","params":{"textDocument":{"uri":"($file_uri)"},"position":{"line":0,"character":($i mod 30)}}}'
} | str join ''

# Start LSP with hover requests in background.
job spawn { ($init + $initialized + $did_open + $hovers) | tg lsp | ignore }

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
