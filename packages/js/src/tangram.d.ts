/// <reference lib="es2023" />

export * from "@tangramdotdev/client";
export as namespace Tangram;
export as namespace tg;

declare global {
	interface ImportMeta {
		module: tg.Module;
	}

	interface Console {
		/** Write to stdout. */
		log(...args: Array<unknown>): void;

		/** Write to stderr. */
		error(...args: Array<unknown>): void;
	}

	var console: Console;

	function Tangram(
		strings: TemplateStringsArray,
		...placeholders: Tangram.Args<Tangram.Template.Arg>
	): Tangram.Template.Builder;
	function Tangram(
		...args: Tangram.Args<Tangram.Template.Arg>
	): Tangram.Template.Builder;

	function tg(
		strings: TemplateStringsArray,
		...placeholders: tg.Args<tg.Template.Arg>
	): tg.Template.Builder;
	function tg(...args: tg.Args<tg.Template.Arg>): tg.Template.Builder;
}
