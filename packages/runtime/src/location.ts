import * as tg from "./index.ts";

export type Location = {
	module: tg.Module;
	range: tg.Range;
};

export namespace Location {
	export type Data = {
		module: tg.Module.Data;
		range: tg.Range;
	};

	export let toData = (value: tg.Location): tg.Location.Data => {
		return {
			module: tg.Module.toData(value.module),
			range: value.range,
		};
	};

	export let fromData = (data: tg.Location.Data): tg.Location => {
		return {
			module: tg.Module.fromData(data.module),
			range: data.range,
		};
	};
}
