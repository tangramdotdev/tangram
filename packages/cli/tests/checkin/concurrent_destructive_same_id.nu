use ../../test.nu *

# Two destructive checkins of identical content race to rename their roots to the same checkout path. The loser must treat the existing destination as already checked out rather than failing.
#
# The destination is only made read-only after the rename, so the loser must be released while the destination is still writable. A rename that is not no-replace reports ENOTEMPTY there rather than the tolerated EEXIST, because the destination is a non-empty directory.

let server = spawn --config {
	advanced: {
		checkpoints: true,
	},
}

let first = artifact { a.txt: 'hello' }
let second = artifact { a.txt: 'hello' }

def checkin_background [path: path] {
	job spawn {
		let job_id = job id
		let output = tg checkin --destructive --no-ignore $path | complete
		$output | job send --tag $job_id 0
	}
}

let rename_watch = (
	tg checkpoint watch checkin.checkout.destructive.rename
	| from json
	| get watch
)
let renamed_watch = (
	tg checkpoint watch checkin.checkout.destructive.renamed
	| from json
	| get watch
)

let checkins = [$first $second] | each { |path| checkin_background $path }

# Park both checkins before they rename their roots.
tg checkpoint wait checkin.checkout.destructive.rename $rename_watch 0 | ignore
tg checkpoint wait checkin.checkout.destructive.rename $rename_watch 1 | ignore

# Let the winner rename, then hold it before it makes the destination read-only.
tg checkpoint continue checkin.checkout.destructive.rename $rename_watch 0
tg checkpoint wait checkin.checkout.destructive.renamed $renamed_watch 0 | ignore

# The loser renames while the destination is still writable.
tg checkpoint continue checkin.checkout.destructive.rename $rename_watch 1

tg checkpoint continue checkin.checkout.destructive.renamed $renamed_watch 0
tg checkpoint unwatch checkin.checkout.destructive.renamed $renamed_watch
tg checkpoint unwatch checkin.checkout.destructive.rename $rename_watch

for checkin in $checkins {
	let output = job recv --tag $checkin --timeout 10sec
	success $output "the losing destructive checkin should succeed"
}
