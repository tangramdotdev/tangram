use ../../test.nu *

# A process must be able to check out a file its command references. The command that grants the process is one hop from the file in both directions, but the searches walk depth first, so the ancestor search climbs the file's directory parent instead of taking the command beside it, and the descendant search walks the command's directory child instead of taking the file beside it. Each abandons a proof it has already enqueued, and the authorization reports an error instead of an answer.

let server = server spawn

let deep = 0..<18 | reduce --fold 'tg.file("loader")' {|_, expr| ['tg.directory({ "a": ', $expr, ' })'] | str join }
tg put $deep
tg index

let path = artifact {
	tangram.ts: '
		export const read = async (bulk: tg.Directory, loader: tg.File) => "ok";

		export default async () => {
			const entries: Record<string, tg.File> = {};
			for (let i = 0; i < 600; i++) {
				entries[`f${i}`] = tg.file(`${i}`);
			}
			return await tg.build(read, await tg.directory(entries), await tg.file("loader"));
		}
	'
}

let output = tg build $path | str trim
assert equal $output '"ok"' 'a process must be able to check out a file its command references'
