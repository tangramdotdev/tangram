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
	let firstArg: tg.Process.RunArgObject = {
		sandbox: true,
		stdout: "log",
		stderr: "log",
		env: {
			TANGRAM_HOST: tg.process.env.TANGRAM_HOST,
		},
	};
	if (typeof args[0] === "function") {
		return new tg.RunBuilder(firstArg, {
			host: "js",
			executable: tg.Command.Executable.fromData(tg.handle.magic(args[0])),
			args: args.slice(1),
		});
	} else if (Array.isArray(args[0]) && "raw" in args[0]) {
		let strings = args[0] as TemplateStringsArray;
		let placeholders = args.slice(1);
		let template = tg.template(strings, ...placeholders);
		let executable = tg.process.env.SHELL ?? "sh";
		tg.assert(tg.Command.Arg.Executable.is(executable));
		return new tg.RunBuilder(firstArg, {
			executable,
			args: ["-c", template],
		});
	} else {
		return new tg.RunBuilder(firstArg, ...args);
	}
}
