use ../lib/test.nu *
use ../lib/lsp.nu

let local = server spawn
let dependency = artifact {
    'tangram.ts': "export { greet } from './helper.tg.ts';"
    'helper.tg.ts': 'export function greet() { return 42; }'
}
tg tag lsp-js $dependency
for specifier in [$dependency 'lsp-js'] {
    let source = ($'import { greet } from "($specifier)";' + "\nconst value = greet();\n")
    let path = artifact {'main.tg.ts': $source}
    let uri = lsp uri ($path | path join main.tg.ts)
    let responses = lsp exchange [
        (lsp initialize 1)
        (lsp initialized)
        (lsp did_open $uri $source)
        (lsp definition 10 $uri 1 16)
    ]
    let location = (lsp result $responses 10).0
    assert equal $location.range {start: {line: 0, character: 16}, end: {line: 0, character: 21}}
    let file = lsp path_for_uri $location.uri
    assert (open --raw $file | str contains 'function greet')
    assert ($file | str ends-with '.tg.ts')
}

# An unsaved import remains available with an incomplete statement elsewhere.
let path = artifact {'main.tg.ts': ''}
let uri = lsp uri ($path | path join main.tg.ts)
let source = ($'import { greet } from "($dependency)";' + "\nconst value = greet();\nconst unfinished =\n")
let responses = lsp exchange [
    (lsp initialize 1)
    (lsp initialized)
    (lsp did_open $uri $source)
    (lsp definition 10 $uri 1 16)
]
assert equal (lsp result $responses 10).0.range.start {line: 0, character: 16}
