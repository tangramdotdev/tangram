import * as tg from "../index.ts";
import * as spawn from "./spawn.ts";

export let builder = (...args: any): any => {
	if (typeof args[0] === "function") {
		return new tg.Process.Builder("exec", {
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
		return new tg.Process.Builder("exec", arg);
	} else {
		return new tg.Process.Builder("exec", ...args);
	}
};

export let execUnsandboxed = async (
	arg: tg.Process.Spawn.Arg,
): Promise<never> => {
	if (arg.sandbox !== undefined) {
		throw new Error("an exec must not be sandboxed");
	}
	validateStdio(arg.stdin ?? "inherit", "stdin");
	validateStdio(arg.stdout ?? "inherit", "stdout");
	validateStdio(arg.stderr ?? "inherit", "stderr");

	tg.assert(typeof tg.process.env.TANGRAM_OUTPUT === "string");
	let prepared = await spawn.prepareUnsandboxedCommand(
		arg,
		tg.process.env.TANGRAM_OUTPUT,
	);
	return await tg.host.exec({
		args: prepared.args,
		cwd: prepared.cwd,
		env: prepared.env,
		executable: prepared.executable,
		stderr: renderStdio(arg.stderr ?? "inherit"),
		stdin: renderStdio(arg.stdin ?? "inherit"),
		stdout: renderStdio(arg.stdout ?? "inherit"),
	});
};

function validateStdio(
	stdio: string,
	stream: "stdin" | "stdout" | "stderr",
): void {
	if (stdio === "inherit" || stdio === "null") {
		return;
	}
	throw new Error(`${stream} must be inherit or null for an exec`);
}

function renderStdio(stdio: string): "inherit" | "null" {
	if (stdio === "inherit" || stdio === "null") {
		return stdio;
	}
	throw new Error("stdio must be inherit or null for an exec");
}
