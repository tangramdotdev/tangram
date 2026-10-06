use ../lib/test.nu *

let local = server spawn
let dependency = artifact {
    'tangram.js': 'export default 0;'
    'module.tg.js': 'export default 42;'
}
tg tag put -p tools/1.0.0 $dependency
let path = artifact {
    'main.tg.js': '
        import root from "tools/^1";
        import value from "tools/^1" with { get: "module.tg.js" };
        export default value;
    '
}
let socket = $local.url | str replace 'http+unix://' '' | url decode
let headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
let body = {
    referrer: {kind: 'js', referent: {node: ($path | path join main.tg.js)}}
    import: {kind: 'js', reference: 'tools/^1?get=module.tg.js'}
} | to json --raw
let output = $body | into binary | http post --headers $headers --unix-socket $socket 'http://localhost/modules/resolve'
assert equal $output.module.kind 'js'
assert equal $output.module.referent.options.path 'module.tg.js'
let body = {module: $output.module} | to json --raw
let output = $body | into binary | http post --headers $headers --unix-socket $socket 'http://localhost/modules/load'
assert equal $output.text 'export default 42;'
