use ../lib/test.nu *

# TTY size writes discover an existing process across multiple candidate locations.
let remote = server spawn --name remote
let client = server spawn --name client --config { remotes: { default: { url: $remote.url } } }
let path = artifact { tangram.ts: 'export default () => "done";' }
let process = tg --url $remote.url spawn --tty=24,80 $path | str trim
tg --url $remote.url wait --source=index $process | ignore
let socket = $client.url | str replace 'http+unix://' '' | url decode
http put --max-time 10sec --unix-socket $socket --content-type application/json $'http://localhost/processes/($process)/tty/size' { location: 'local,remote', size: { rows: 40, cols: 100 } } | ignore
