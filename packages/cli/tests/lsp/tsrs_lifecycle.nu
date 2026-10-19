use ../lib/test.nu *
use ../lib/lsp.nu

$env.TSRS_REGION_LOG = '1'
let local = server spawn
let source = 'import { value } from "./helper.tg.ts"; export const answer: number = value; answer;'
let character = ($source | str index-of --end answer) + 1
let path = artifact {'main.tg.ts': $source, 'helper.tg.ts': 'export const value = 42;'}
let uri = lsp uri ($path | path join main.tg.ts)
mut session = lsp start
$session = lsp send_all $session [
    (lsp initialize 1)
    (lsp initialized)
    (lsp did_open $uri $source)
    (lsp hover 10 $uri 0 $character)
    (lsp request 11 'textDocument/completion' {textDocument: {uri: $uri}, position: {line: 0, character: $character}})
]
let response = lsp wait_result $session 10
$session = $response.session
assert ($response.result.contents.value | str contains 'number')
let response = lsp wait_result $session 11
$session = $response.session
let item = $response.result | where label == answer | first
$session = lsp send_all $session [
    (lsp request 12 'completionItem/resolve' $item)
    (lsp diagnostics 13 $uri)
]
let response = lsp wait_result $session 12
$session = $response.session
assert ($response.result.detail | str contains 'number')
let response = lsp wait_result $session 13
$session = $response.session
assert equal $response.result.items []
$session = lsp send_all $session (20..59 | each {|id| lsp hover $id $uri 0 $character })
for id in 20..59 {
    let response = lsp wait_result $session $id
    $session = $response.session
    assert ($response.result.contents.value | str contains 'number')
}

# Unchanged requests share one native program, including completion preparation and resolution.
let deadline = (date now) + 5sec
loop {
    let created = open --raw $local.log | lines | where {|line| ($line | str contains 'regions: program ') and ($line | str contains ': created ') } | length
    if $created > 0 {
        assert equal $created 1
        break
    }
    assert ((date now) < $deadline)
    sleep 20ms
}

# Completion preparation for another file must preserve earlier completion state.
let other_uri = lsp uri ($path | path join other.tg.ts)
$session = lsp send_all $session [
    (lsp did_open $other_uri 'export const tsrsLifecycleOther = 1; tsrsLifecycleOther;')
    (lsp request 70 'textDocument/completion' {textDocument: {uri: $other_uri}, position: {line: 0, character: 40}})
    (lsp request 71 'textDocument/completion' {textDocument: {uri: $uri}, position: {line: 0, character: $character}})
    (lsp request 72 'completionItem/resolve' $item)
]
let response = lsp wait_result $session 70
$session = $response.session
assert ('tsrsLifecycleOther' in $response.result.label)
let response = lsp wait_result $session 71
$session = $response.session
assert ('answer' in $response.result.label)
let response = lsp wait_result $session 72
$session = $response.session
assert ($response.result.detail | str contains 'number')
$session = lsp send_all $session [
    (lsp notification 'textDocument/didClose' {textDocument: {uri: $other_uri}})
    (lsp request 73 'workspace/symbol' {query: 'tsrsLifecycleOther'})
]
let response = lsp wait_result $session 73
$session = $response.session
assert equal $response.result []

# A changed buffer updates the retained project before the next query.
let changed = 'export const answer: string = "yes"; answer;'
$session = lsp send_all $session [
    (lsp notification 'textDocument/didChange' {textDocument: {uri: $uri, version: 2}, contentChanges: [{text: $changed}]})
    (lsp hover 60 $uri 0 38)
    (lsp diagnostics 61 $uri)
]
let response = lsp wait_result $session 60
$session = $response.session
assert ($response.result.contents.value | str contains 'string')
let response = lsp wait_result $session 61
$session = $response.session
assert equal $response.result.items []

# A failed request must leave the retained service usable.
$session = lsp send_all $session [
    (lsp hover 62 (lsp uri ($path | path join missing.tg.ts)) 0 0)
    (lsp hover 63 $uri 0 38)
]
mut failed = false
loop {
    let response = lsp read $session
    $session = $response.session
    if $response.message.id? == 62 {
        assert ($response.message.error? != null)
        $failed = true
        break
    }
    $session = $session | update inbox ($session.inbox | append $response.message)
}
assert $failed
let response = lsp wait_result $session 63
$session = $response.session
assert ($response.result.contents.value | str contains 'string')
lsp stop $session
