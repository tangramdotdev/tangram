import * as tg from "./index.ts";

export function build<
	A extends tg.UnresolvedArgs<Array<tg.Value>>,
	R extends tg.ReturnValue,
>(function_: (...args: A) => R): tg.RunBuilder<[], tg.ResolvedReturnValue<R>>;
export function build<
	A extends tg.UnresolvedArgs<Array<tg.Value>>,
	R extends tg.ReturnValue,
>(
	function_: (...args: A) => R,
	...args: tg.UnresolvedArgs<tg.ResolvedArgs<A>>
): tg.RunBuilder<[], tg.ResolvedReturnValue<R>>;
export function build(
	strings: TemplateStringsArray,
	...placeholders: tg.Args<tg.Template.Arg>
): tg.RunBuilder;
export function build(...args: tg.Args<tg.Process.RunArg>): tg.RunBuilder;
export function build(...args: any): any {
	let validate = (arg: tg.Process.RunArgObject): void => {
		let cacheable =
			(arg.mounts?.length ?? 0) === 0 &&
			(arg.network ?? false) === false &&
			arg.stdin === "null" &&
			arg.stdout === "log" &&
			arg.stderr === "log" &&
			(arg.tty === undefined || arg.tty === false);
		cacheable = cacheable || arg.checksum !== undefined;
		if (!cacheable) {
			throw tg.error("a build must be cacheable");
		}
	};
	let firstArg: tg.Process.RunArgObject = {
		sandbox: true,
		stderr: "log",
		stdin: "null",
		stdout: "log",
		tty: false,
		env: {
			TANGRAM_HOST: tg.process.env.TANGRAM_HOST,
		},
	};
	if (typeof args[0] === "function") {
		return new tg.RunBuilder(firstArg, {
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
		return new tg.RunBuilder(firstArg, {
			executable,
			args: ["-c", template],
		}).validate(validate);
	} else {
		return new tg.RunBuilder(firstArg, ...args).validate(validate);
	}
}
