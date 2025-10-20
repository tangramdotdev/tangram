import type { Module } from "./module.ts";
import type { Range } from "./range.ts";

export type Location = {
	module: Module;
	range: Range;
};
