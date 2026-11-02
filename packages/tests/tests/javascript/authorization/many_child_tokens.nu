use ../../../util/test.nu *

# Creating commands that reference a loaded directory must take time linear in the directory's entries, even when its entries hold more distinct tokens than fit in a cache.

def files [prefix: string, count: int] {
	0..<$count | each {|index| { $'f($index)': $'($prefix) ($index)' } } | into record
}

let local = server spawn

let path = artifact {
	tangram.ts: '
		import small from "./small" with { type: "directory" };
		import large from "./large" with { type: "directory" };

		export default async function () {
			let time = async (directory: tg.Directory, count: number) => {
				let start = Date.now();
				for (let index = 0; index < count; index++) {
					let command = await tg.command({
						args: [tg`${directory}/f${String(index)}`],
						executable: "sh",
						host: tg.host.current,
					});
					command.id;
				}
				return Date.now() - start;
			};

			// Load both directories, so that each entry holds its own token.
			await small.entries;
			await large.entries;

			// Warm up, then time creating the commands in alternating rounds, keeping the fastest round for each directory.
			await time(small, 100);
			let smallElapsed = Infinity;
			let largeElapsed = Infinity;
			for (let round = 0; round < 3; round++) {
				smallElapsed = Math.min(smallElapsed, await time(small, 400));
				largeElapsed = Math.min(largeElapsed, await time(large, 400));
			}
			return { largeElapsed, smallElapsed };
		}
	'
	small: (files small 500)
	large: (files large 2000)
}

let output = tg build $path | from json
print $output
print $'ratio: ($output.largeElapsed / $output.smallElapsed)'

# The large directory has four times as many entries, so its commands should take about four times as long.
assert ($output.largeElapsed < ($output.smallElapsed * 8))
