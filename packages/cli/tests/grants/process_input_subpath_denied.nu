use ../../test.nu *

# Reaching a descendant of a process's own input by subpath is refused, while every other route to the same file works: the process may read the whole tree, which includes that file, and may read the containing directory by id. Only the one request form is denied, so this is an inconsistency in that path rather than a limit on what the process is entitled to. Note `try_get_apply_get` in packages/server/src/get.rs stores the traversed artifact, which the other two routes do not do.

let server = server spawn --busybox

let path = artifact { tangram.ts: '
	import busybox from "busybox";
	export default async function () {
		const dir = await tg.directory({ "d": tg.directory({ "leaf.txt": tg.file("leaf") }) });
		return tg.run`
			target=$(basename ${dir})
			if tg get "$target?get=d/leaf.txt" > /dev/null 2>&1; then a=ok; else a=denied; fi
			if tg get "$target" --depth 3 > /dev/null 2>&1; then b=ok; else b=denied; fi
			child=$(tg get "$target" --depth 1 | grep -o "dir_[a-z0-9]*" | head -1)
			if tg get "$child" --depth 2 > /dev/null 2>&1; then c=ok; else c=denied; fi
			echo "$a $b $c" > $TANGRAM_OUTPUT
		`.env(tg.build(busybox)).then(tg.File.expect);
	}
' }

let results = tg build $path | str trim | tg cat $in | str trim | split row ' '
assert equal ($results | get 1) 'ok' "a process must be able to read its whole input"
assert equal ($results | get 2) 'ok' "a process must be able to read a directory inside its input by id"
assert equal ($results | first) 'ok' "a process must be able to read a file inside its input by subpath"
