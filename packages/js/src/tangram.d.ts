/// <reference lib="es2023" />

export * from "@tangramdotdev/client";
export as namespace Tangram;
export as namespace tg;

declare global {
	interface ImportMeta {
		module: tg.Module;
	}

	let console: {
		/** Write to stdout. */
		log: (...args: Array<unknown>) => void;

		/** Write to stderr. */
		error: (...args: Array<unknown>) => void;
	};

	function Tangram(
		strings: TemplateStringsArray,
		...placeholders: Tangram.Args<Tangram.Template.Arg>
	): Promise<Tangram.Template>;
	function Tangram(
		...args: Tangram.Args<Tangram.Template.Arg>
	): Promise<Tangram.Template>;

	function tg(
		strings: TemplateStringsArray,
		...placeholders: tg.Args<tg.Template.Arg>
	): Promise<tg.Template>;
	function tg(...args: tg.Args<tg.Template.Arg>): Promise<tg.Template>;
}
