import { Module } from "./module.ts";
import { Range } from "./range.ts";

export type Location = {
	module: Module;
	range: Range;
};
