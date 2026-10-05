import * as tg from "@tangramdotdev/client";

type Arg = {
	args: Array<tg.Value.Data>;
	cwd: string;
	env: Record<string, tg.Value.Data>;
	export: string | null;
	module: tg.Module.Data;
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
	tg.setProcess({
		args,
		cwd,
		env,
		export: arg.export,
		module: tg.Module.fromData(arg.module),
	});
};
