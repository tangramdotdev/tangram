use ../lib/test.nu *
use ../lib/archive.nu *

# A put whose archive and index batches span several indexer requests is archived and indexed.

skip_test --cloud
let archive = spawn_archive
let local = server spawn --cloud --config {
	archive: $archive.config,
	indexer: { batch: { request_size: 128 } },
	object: {
		archive_queue: { fragment_size: 64 },
		index_queue: { fragment_size: 64 },
	},
}

# Put a directory of eight files, which stores seventeen objects.
let files = 0..<8 | each { |i| $'"($i).txt":' + 'tg.file("' + ($i | into string) + '")' } | str join ','
let output = ('tg.directory({' + $files + '})') | tg --url $local.url put --no-tokens | complete
success $output 'the directory must be stored'
let id = $output.stdout | referent node

# Every object must be uploaded to the archive. Respond to each upload as it arrives, because the archive client holds a limited number of connections.
for count in 1..17 {
	http get --max-time 30sec $'($archive.url)/requests?count=($count)' | ignore
	http post $'($archive.url)/respond' '200' | ignore
}
let requests = http get --max-time 30sec $'($archive.url)/requests?count=17'
let paths = $requests | get path | uniq
assert equal ($paths | length) 17 'every object must be uploaded to the archive'
assert ($'/($id)' in $paths) 'the directory must be uploaded to the archive'

# The directory tree must be indexed.
tg --url $local.url index
let metadata = tg --url $local.url object metadata $id | from json
assert equal $metadata.subtree.count 17 'the directory tree must be indexed'

server stop $local
job kill $archive.job
