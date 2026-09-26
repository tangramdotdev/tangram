use ../lib/test.nu *
use ../lib/lsp.nu

# An LSP session can check an edited document after exhausting the compiler's heap.

let server = server spawn

# Each alias expands to 90,000 distinct strings to exhaust the default V8 heap.
let aliases = 0..999 | each { |i| $'type A($i) = `($i)${T}`;' }
let path = artifact {
	tangram.ts: ([
		'type D = "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9";'
		'type E = "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9";'
		'type T = `${D}${D}${D}${D}${E}`;'
		...$aliases
		'export default function () {}'
	] | str join "\n")
}
let module_path = $path | path join tangram.ts
let module_uri = lsp uri $module_path

mut client = lsp start
$client = lsp send $client (lsp initialize 1)
let output = lsp wait_result $client 1
$client = $output.session
$client = lsp send_all $client [
	(lsp initialized)
	(lsp did_open $module_uri (open $module_path))
	(lsp diagnostics 10 $module_uri)
]

let output = lsp read $client --timeout 1min
$client = $output.session
assert equal $output.message.id 10
assert equal $output.message.error.code (-32603)
assert equal $output.message.error.message 'the compiler ran out of memory'

# Keep the edit in memory so the replacement isolate must use the open document.
$client = lsp send_all $client [
	(lsp notification "textDocument/didChange" {
		textDocument: { uri: $module_uri, version: 2 },
		contentChanges: [{ text: 'export default function () { return "Hello, World!"; }' }],
	})
	(lsp diagnostics 20 $module_uri)
]
let output = lsp wait_result $client 20
$client = $output.session
assert equal $output.result.kind 'full'
assert equal $output.result.items []
lsp stop $client
