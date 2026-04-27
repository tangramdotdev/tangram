import * as tg from "@tangramdotdev/client";

type Arg = {
	args: Array<tg.Value.Data>;
	cwd: string;
	env: Record<string, tg.Value.Data>;
	executable: tg.Command.Data.Executable;
};

export let init = (arg: Arg) => {
	let args = arg.args.map(tg.Value.fromData);
	let cwd = arg.cwd;
	let env = Object.fromEntries(
		Object.entries(arg.env).map(([key, value]) => [
			key,
			tg.Value.fromData(value),
		]),
	);
	let executable = tg.Command.Executable.fromData(arg.executable);
	tg.setProcess({
		args,
		cwd,
		env,
		executable,
	});
};
