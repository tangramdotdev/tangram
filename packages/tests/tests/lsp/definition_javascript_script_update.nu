use ../../util/test.nu *
use ../../util/lsp.nu

# TypeScript go-to-definition into a tagged dependency of a single-file script follows dependency updates.

let local = server spawn
let dependency = artifact {'tangram.ts': 'export function greet() { return 42; }'}
tg tag -p lsp-script/1.0.0 $dependency
rm --recursive $dependency
let path = artifact {'main.tg.ts': 'import { greet } from "lsp-script/^1";
const value = greet();
'}
let uri = lsp uri ($path | path join main.tg.ts)
mut session = lsp start
$session = lsp send_all $session [(lsp initialize 1) (lsp initialized)]
let response = lsp wait_result $session 1
$session = $response.session

$session = lsp send $session (lsp definition 10 $uri 1 16)
let response = lsp wait_result $session 10
$session = $response.session
assert equal $response.result.0.range.start {line: 0, character: 16}

# A dependency update must invalidate the resolution.
let updated = artifact {'tangram.ts': "// The updated package.\n\nexport function greet() { return 43; }"}
tg tag -p lsp-script/1.1.0 $updated
rm --recursive $updated
tg index
success (tg update ($path | path join main.tg.ts) | complete)
$session = lsp send $session (lsp definition 11 $uri 1 16)
let response = lsp wait_result $session 11
$session = $response.session
assert equal ($response.result | length) 1
assert equal $response.result.0.range.start {line: 2, character: 16}
lsp stop $session
