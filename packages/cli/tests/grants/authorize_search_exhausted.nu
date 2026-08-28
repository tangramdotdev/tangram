use ../../test.nu *

# A build must not fail because of unrelated content in the store. The same build succeeds against a clean store, then fails once unrelated artifacts contain the same file and exhaust the authorization search.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export const work = async (_big: tg.Directory, _loader: tg.File, _mark: string) => "ok";

		export default async (mark: string) => {
			const entries: Record<string, tg.File> = {};
			for (let i = 0; i < 512; i++) {
				entries[`e${i}`] = await tg.file(`big${i}`);
			}
			const big = await tg.directory(entries);
			return await tg.build(work, big, tg.file("loader"), mark);
		}
	'
}

let output = tg build $path -a clean | complete
success $output 'the build must succeed against a clean store'

# Accumulate unrelated artifacts that contain the same file.
let entries = 0..<1200 | each { |i|
	let n = $i | into string
	['"d' $n '": tg.directory({"f' $n '": tg.file("loader")})'] | str join
} | str join ','
tg put (['tg.directory({' $entries '})'] | str join) | ignore
tg index

let output = tg build $path -a ambient | complete
success $output 'a build must not fail because unrelated artifacts exhausted the authorization search'
