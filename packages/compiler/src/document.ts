import { todo } from "./assert.ts";
import type { Module } from "./module.ts";

export type Request = {
	module: Module;
};

export type Response = {
	exports: { [key: string]: any };
};

export let handle = (_request: Request): Response => {
	return todo();
};
