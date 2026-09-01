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

export def token-resource [module: record] {
	$module.referent.options.tokens.local
		| split row '.'
		| get 1
		| decode base64
		| decode utf-8
		| from json
		| get resource
}
