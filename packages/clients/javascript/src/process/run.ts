import * as tg from "../index.ts";
import * as attach from "./attach.ts";

export let builder = (...args: any): any => {
	if (
		typeof args[0] === "function" &&
		!(args[0] instanceof tg.Command.Builder)
	) {
		let command = tg.Command.javascriptArg(args[0], args.slice(1)).then(
			(command) => ({
				command,
			}),
		);
		return new tg.Process.Builder("run", command);
	} else if (Array.isArray(args[0]) && "raw" in args[0]) {
		let strings = args[0] as TemplateStringsArray;
		let placeholders = args.slice(1);
		let template = tg.template(strings, ...placeholders);
		let executable = tg.process.env.SHELL ?? "sh";
		tg.assert(tg.Command.Arg.Executable.is(executable));
		let arg = {
			executable,
			args: ["-c", template],
		};
		return new tg.Process.Builder("run", arg);
	} else {
		return new tg.Process.Builder("run", ...args);
	}
};

export async function run<O extends tg.Value>(
	arg: tg.Process.Spawn.Arg,
	options: tg.Referent.Options,
	echo = false,
): Promise<O> {
	let process = await attach.spawn<O>(arg, options, "run");
	if (!echo) {
		return process.output();
	}

	// Await the output while echoing the logged streams, and finish echoing before returning, even if the process fails.
	let [output, echoed] = await Promise.allSettled([
		process.output(),
		echoStdio(process, arg),
	]);
	if (output.status === "rejected") {
		throw output.reason;
	}
	if (echoed.status === "rejected") {
		throw echoed.reason;
	}
	return output.value;
}

/** Echo a process's logged stdout and stderr to this process's stdout and stderr, from the beginning of its log until it ends. */
async function echoStdio(
	process: tg.Process,
	arg: tg.Process.Spawn.Arg,
): Promise<void> {
	let streams: Array<tg.Process.Stdio.Stream> = [];
	if (arg.stdout === "log") {
		streams.push("stdout");
	}
	if (arg.stderr === "log") {
		streams.push("stderr");
	}
	if (streams.length === 0) {
		return;
	}
	let iterator = await process.readStdio({ streams });
	for await (let chunk of iterator) {
		let fd = chunk.stream === "stdout" ? 1 : 2;
		await tg.host.write(fd, chunk.bytes);
	}
}
