use ../lib/test.nu *

let server = server spawn
let file = tg put --no-tokens 'tg.file("hello")' | str trim
let socket = $server.url | str replace 'http+unix://' '' | url decode
let response = http post --allow-errors --raw --content-type application/json --unix-socket $socket http://localhost/push { nodes: [$file] }
assert ($response | str contains 'a push requires a destination') 'a push must specify its destination'
