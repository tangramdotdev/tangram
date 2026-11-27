use ../../util/test.nu *

# A server whose log reaches its maximum size starts a new file, keeps at most the maximum number of files, and keeps writing to the newest.

let directory = mktemp -d
let config_path = $directory | path join 'config.json'
{
	log: { max_files: 3, max_size: 1024 },
	tracing: { filter: 'tangram_http=trace' },
	vfs: false,
} | to json | save $config_path
let server_directory = $directory | path join 'server'
let tangram = [--config $config_path --directory $server_directory --no-remotes]

let output = tangram ...$tangram server start | complete
success $output 'the server should start'

# Write several times the maximum size to the log by making requests whose paths the server logs.
for index in 1..20 {
	tangram ...$tangram tag get $'marker-($index)' | complete | ignore
}

let output = tangram ...$tangram server stop | complete
success $output 'the server should stop'

# The log has rotated and kept at most three files.
let files = ls ($server_directory | path join 'log') | get name | path basename | sort
assert (($files | length) >= 2) $'the log should have rotated: ($files)'
assert (($files | length) <= 3) $'the log should keep at most three files: ($files)'
assert ($files | all { |name| $name =~ '^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z(_\d{6})?\.log$' }) $'the files should be named by their start times: ($files)'

# Every file but the newest reached the maximum size before the server moved to the next one.
let sizes = $files | drop 1 | each { |name| ls ($server_directory | path join 'log' $name) | get 0.size | into int }
assert ($sizes | all { |size| $size >= 1024 }) $'every file but the newest should have reached the maximum size: ($sizes)'

# The log has the requests in order through the last one, and the oldest requests were removed with their files.
let output = tangram ...$tangram server log | complete
success $output
let markers = $output.stdout | parse --regex '/tags/marker-(\d+)"' | get capture0 | into int | uniq
assert equal ($markers | last) 20 'the log should end with the last request'
assert equal $markers ($markers | sort) 'the log should have the requests in order'
assert ($markers.0 > 1) 'the log should not have the first request'
