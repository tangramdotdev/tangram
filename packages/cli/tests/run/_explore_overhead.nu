use ../../test.nu *

# Throwaway exploratory benchmark. Decomposes the per-process tax beyond bare
# sandbox spawn: mounting a large shared input (like the toolchain), mounting a
# directory of many files (like a deps dir), and checking an output back out.
# Each scenario spawns `count` DISTINCT sandboxed processes sequentially; the
# command varies per iteration (echo of the index) so none are cache hits,
# while the mounted input is held constant.

const count = 20

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async (mode: string, count: string, size: string) => {
			const n = Number(count);
			const sz = Number(size);
			if (mode === "spawn") {
				for (let i = 0; i < n; i++) {
					await tg.run`true; echo ${`${i}`}`.sandbox();
				}
			} else if (mode === "input") {
				const big = await tg.file("x".repeat(sz));
				for (let i = 0; i < n; i++) {
					await tg.run`cat ${big} > /dev/null; echo ${`${i}`}`.sandbox();
				}
			} else if (mode === "inputdir") {
				const entries: Record<string, string> = {};
				for (let j = 0; j < sz; j++) { entries[`f${j}`] = "y"; }
				const dir = await tg.directory(entries);
				for (let i = 0; i < n; i++) {
					await tg.run`ls ${dir} > /dev/null; echo ${`${i}`}`.sandbox();
				}
			} else if (mode === "output") {
				for (let i = 0; i < n; i++) {
					await tg.run`head -c ${`${sz}`} /dev/zero > ${tg.output}; true ${`${i}`}`.sandbox();
				}
			}
		};
	',
}

def measure_mode [label: string, mode: string, size: string, path: string, baseline: duration] {
	let start = date now
	let out = tg run $path -a $mode -a $"($count)" -a $size | complete
	success $out $"($label) should succeed"
	let elapsed = (date now) - $start
	print -e $"($label): total=($elapsed) per_process=(($elapsed - $baseline) / $count)"
}

# Fixed overhead: outer process + JS runtime, zero inner processes.
let baseline_start = date now
let baseline_output = tg run $path -a 'spawn' -a '0' -a '0' | complete
success $baseline_output "baseline should succeed"
let baseline = (date now) - $baseline_start
print -e $"baseline \(outer process, n=0\): ($baseline)"

measure_mode "spawn only          " "spawn"    "0"        $path $baseline
measure_mode "input 1MB file      " "input"    "1048576"  $path $baseline
measure_mode "input 4MB file      " "input"    "4194304"  $path $baseline
measure_mode "input dir 500 files " "inputdir" "500"      $path $baseline
measure_mode "output 1MB          " "output"   "1048576"  $path $baseline
