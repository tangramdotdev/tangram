use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
let first = artifact {'tangram.ts': 'export function greet() { return 42; }'}
tg tag -p incremental-js/1.0.0 $first
let path = artifact {
    'tangram.ts': 'export {};'
    'main.tg.ts': 'import { greet } from "incremental-js/^1";
const value = greet();'
}
tg checkin $path | ignore
# Exercise distinct lockfile timestamps within the same second.
python3 -c 'import os, sys; os.utime(sys.argv[1], ns=(946684800100000000, 946684800100000000))' ($path | path join tangram.lock)
let uri = lsp uri ($path | path join main.tg.ts)
mut session = lsp start
$session = lsp send_all $session [(lsp initialize 1) (lsp initialized) (lsp did_open $uri (open --raw ($path | path join main.tg.ts)))]
let response = lsp wait_result $session 1
$session = $response.session
$session = lsp send $session (lsp definition 10 $uri 1 16)
let response = lsp wait_result $session 10
$session = $response.session
assert equal $response.result.0.range.start {line: 0, character: 16}

let second = artifact {'tangram.ts': '// The updated package.

export function greet() { return 43; }'}
tg tag -p incremental-js/1.1.0 $second
tg index
success (tg update $path | complete)
python3 -c 'import os, sys; os.utime(sys.argv[1], ns=(946684800200000000, 946684800200000000))' ($path | path join tangram.lock)
$session = lsp send $session (lsp definition 11 $uri 1 16)
let response = lsp wait_result $session 11
$session = $response.session
assert equal ($response.result | length) 1
assert equal $response.result.0.range.start {line: 2, character: 16}
let target = lsp path_for_uri $response.result.0.uri
assert (open --raw $target | str contains 'return 43')
lsp stop $session
