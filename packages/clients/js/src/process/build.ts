import * as tg from "../index.ts";
import * as spawn from "./spawn.ts";

export let builder = (...args: any): any => {
	let validate = (arg: tg.Process.ArgObject): void => {
		let sandbox = arg.sandbox ?? true;
		let sandboxArg = spawn.isSandboxArg(sandbox) ? sandbox : undefined;
		let network =
			"network" in arg
				? (arg.network ?? false)
				: (sandboxArg?.network ?? false);
		let cacheable =
			sandbox !== false &&
			typeof sandbox !== "string" &&
			(sandboxArg?.mounts?.length ?? 0) === 0 &&
			(arg.mounts?.length ?? 0) === 0 &&
			!spawn.isNetworkEnabled(network) &&
			arg.stdin === "null" &&
			arg.stdout === "log" &&
			arg.stderr === "log" &&
			(arg.tty === undefined || arg.tty === false);
		cacheable = cacheable || arg.checksum !== undefined;
		if (!cacheable) {
			throw tg.error("a build must be cacheable");
		}
	};
	let firstArg: tg.Process.ArgObject = {
		sandbox: true,
		stderr: "log",
		stdin: "null",
		stdout: "log",
		tty: false,
		env: {
			TANGRAM_HOST: spawn.defaultHost(),
		},
	};
	if (typeof args[0] === "function") {
		return new tg.Process.Builder("run", firstArg, {
			host: "js",
			executable: tg.Command.Executable.fromData(tg.host.magic(args[0])),
			args: args.slice(1),
		}).validate(validate);
	} else if (Array.isArray(args[0]) && "raw" in args[0]) {
		let strings = args[0] as TemplateStringsArray;
		let placeholders = args.slice(1);
		let template = tg.template(strings, ...placeholders);
		let executable = tg.process.env.SHELL ?? "sh";
		tg.assert(tg.Command.Arg.Executable.is(executable));
		return new tg.Process.Builder("run", firstArg, {
			executable,
			args: ["-c", template],
		}).validate(validate);
	} else {
		return new tg.Process.Builder("run", firstArg, ...args).validate(validate);
	}
};
