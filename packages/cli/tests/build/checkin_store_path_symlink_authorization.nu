use ../lib/test.nu *

# A symlink authorization token does not authorize an unmaterialized artifact or an ID mentioned only in its path.

let server = server spawn --config {
	verification: {
		permissions: {
			final: false
			initial: false
		}
	}
	vfs: false
}

let secret = tg put --no-tokens 'tg.directory({ "secret": "contents" })' | referent node

let path = artifact {
	tangram.ts: '
		export default async function (secret: string) {
			const directory = await tg.directory({});
			for (const input of [tg.symlink({ artifact: directory }), tg.symlink(secret)]) {
				const output = await tg.build`
					if tg checkin --no-tokens "\${INPUT%/*}/${secret}" > ${tg.output} 2>&1; then
						exit 1
					fi
				`.env({ INPUT: input }).then(tg.File.expect);
				tg.assert((await output.text).includes("the authorization search exhausted"));
			}
		}
	'
}

let output = tg build $path --arg-string $secret | complete
success $output "the process must not check in an unrelated directory through a symlink authorization token"
