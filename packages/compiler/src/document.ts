import { todo } from "./assert.ts";

export type Request = {
	module: string;
};

export type Response = {
	exports: { [key: string]: any };
};

export let handle = (_request: Request): Response => {
	return todo();
};
