import * as tg from "../index.ts";
import * as connect from "./connect.ts";

export let builder = (...args: any): any => {
	if (typeof args[0] === "function") {
		let command = tg.Command.js(args[0], args.slice(1)).then((command) => ({
			command,
		}));
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
): Promise<O> {
	let process = await connect.spawn<O>(arg, options, "run");
	return process.output();
}
