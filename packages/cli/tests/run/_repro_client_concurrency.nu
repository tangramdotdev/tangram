use ../../test.nu *

# Throwaway reproduction probe: launch many concurrent tg CLIENT processes (like
# cargo spawning ~256 rustc, each its own tgrustc client doing spawn+wait+finish)
# and read the server's write-permit acquisition summary to see whether the
# process-store write permit contends the way the real testProxy build does
# (avg >5ms, max in the seconds).

let clients = ($env.TG_CLIENTS? | default "128" | into int)
let children = ($env.TG_CHILDREN? | default "1" | into int)
let sleep_secs = ($env.TG_SLEEP? | default "1")
let concurrency = ($env.TG_CONC? | default "13" | into int)

let server = spawn --busybox --config {
	runner: {
		concurrency: $concurrency,
	},
	tokio_single_threaded: false,
	tracing: {
		filter: "warn",
	},
}

let path = artifact {
	tangram.ts: '
		import busybox from "busybox";
		export default async (arg: string) => {
			const { salt, count, sleep } = JSON.parse(arg);
			const env = await tg.build(busybox);
			const cs = [];
			for (let i = 0; i < count; i++) {
				const s = String(sleep);
				const tag = salt + "_" + i;
				cs.push(tg.run`sleep ${s}; true ${tag}`.env(env).sandbox());
			}
			await Promise.all(cs);
			return "done";
		};
	',
}

# Warm the outer package and busybox build once so they are not part of the
# timed burst.
tg run $path -a ({ salt: "warm", count: 1, sleep: "0" } | to json) | complete | ignore

# Launch `clients` concurrent client processes, each spawning `children` unique
# sandboxed processes.
let start = date now
1..$clients | par-each --threads $clients { |c|
	let arg = { salt: $"c($c)", count: $children, sleep: $sleep_secs } | to json
	tg run $path -a $arg | complete
} | ignore
let total = (date now) - $start

print -e $"clients=($clients) children=($children) sleep=($sleep_secs)s concurrency=($concurrency) total=($total)"
