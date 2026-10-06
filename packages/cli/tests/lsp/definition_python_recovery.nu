use ../lib/test.nu *
use ../lib/lsp.nu

const response_timeout = 30sec

let local = server spawn
let path = artifact {
    'main.tg.py': '# /// script
# [tool.tangram.imports.dep]
# specifier = "./helper.tg.ts"
# ///
from dep import greet
value = greet()'
    'helper.tg.ts': 'export function'
    'other.tg.py': "def other(): return 42\nvalue = other()"
}
let uri = lsp uri ($path | path join main.tg.py)
let other_uri = lsp uri ($path | path join other.tg.py)
mut session = lsp start
$session = lsp send_all $session [(lsp initialize 1) (lsp initialized)]
let response = lsp wait_result $session 1 --timeout $response_timeout
$session = $response.session
$session = lsp send $session (lsp definition 10 $uri 5 10)
loop {
    let response = lsp read $session --timeout $response_timeout
    $session = $response.session
    if $response.message.id? == 10 {
        assert ($response.message.error? != null)
        break
    }
}

# An error in one query must not poison the retained checker for another module.
$session = lsp send $session (lsp definition 11 $other_uri 1 10)
let response = lsp wait_result $session 11 --timeout $response_timeout
$session = $response.session
assert equal $response.result.0.range.start {line: 0, character: 4}

# A previously failed load remains tracked and is retried when its source changes.
'export function greet() { return 43; }' | save --force ($path | path join helper.tg.ts)
$session = lsp send $session (lsp definition 12 $uri 5 10)
let response = lsp wait_result $session 12 --timeout $response_timeout
$session = $response.session
let location = $response.result.0
let generated = lsp path_for_uri $location.uri
assert ($generated | str contains '/generated/')
assert ($generated | str ends-with '.tg.py')
let line = open --raw $generated | lines | get $location.range.start.line
assert equal ($line | str substring $location.range.start.character..<$location.range.end.character) 'greet'
assert ($line | str contains 'async def greet(')
lsp stop $session
