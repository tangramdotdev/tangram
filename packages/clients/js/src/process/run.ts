import * as tg from "../index.ts";

export let builder = (...args: any): any => {
	if (typeof args[0] === "function") {
		return new tg.Process.Builder("run", {
			host: tg.host.current,
			executable: tg.Command.Executable.fromData(tg.host.magic(args[0])),
			args: args.slice(1),
		});
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
