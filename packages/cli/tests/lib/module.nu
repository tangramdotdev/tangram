export def resolve-module [socket: string, token: string, referrer: record, reference: string] {
	let body = {
		referrer: $referrer
		import: { reference: $reference }
	} | to json --raw
	let headers = {
		'Accept': 'application/json'
		'Authorization': $'Bearer ($token)'
		'Content-Type': 'application/json'
	}
	$body
		| into binary
		| http post --headers $headers --unix-socket $socket 'http://localhost/modules/resolve'
}

export def resolve-module-background [socket: string, token: string, referrer: record, reference: string] {
	job spawn {
		let job_id = job id
		let output = resolve-module $socket $token $referrer $reference
		$output | job send --tag $job_id 0
	}
}

# Read an object as a user through only the tokens that a resolved module carries.
export def read-with-module-tokens [token: string, id: string, module: record] {
	let query = $module.referent.options.tokens.local
		| enumerate
		| each { |entry| $'tokens[local][($entry.index)]=($entry.item | url encode --all)' }
		| str join '&'
	tg --token $token get $'($id)?($query)' | complete
}
