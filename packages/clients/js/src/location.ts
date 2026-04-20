import type * as tg from "./index.ts";

export type Location = Location.Local | Location.Remote;

export namespace Location {
	export type Local = {
		region?: string | undefined;
	};

	export namespace Local {
		export let is = (value: unknown): value is Location.Local => {
			return (
				typeof value === "object" &&
				value !== null &&
				!Array.isArray(value) &&
				!("components" in value) &&
				!("name" in value) &&
				!("regions" in value) &&
				!("remote" in value) &&
				("region" in value
					? value.region === undefined || typeof value.region === "string"
					: true)
			);
		};
	}

	export type Remote = {
		name: string;
		region?: string | undefined;
	};

	export namespace Remote {
		export let is = (value: unknown): value is Location.Remote => {
			return (
				typeof value === "object" &&
				value !== null &&
				!Array.isArray(value) &&
				!("components" in value) &&
				!("regions" in value) &&
				!("remote" in value) &&
				"name" in value &&
				typeof value.name === "string" &&
				("region" in value
					? value.region === undefined || typeof value.region === "string"
					: true)
			);
		};
	}

	export let is = (value: unknown): value is Location => {
		return Local.is(value) || Remote.is(value);
	};

	export let toDataString = (value: tg.Location): string => {
		let arg = Arg.fromLocation(value);
		return Arg.toDataString(arg);
	};

	export let fromDataString = (data: string): tg.Location => {
		let arg = Arg.fromDataString(data);
		let location = Arg.toLocation(arg);
		if (location === undefined) {
			throw new Error("expected exactly one location");
		}
		return location;
	};

	export type Arg = {
		components: Array<tg.Location.Arg.Component>;
	};

	export namespace Arg {
		export type Component =
			| tg.Location.Arg.LocalComponent
			| tg.Location.Arg.RemoteComponent;

		export namespace Component {
			export let is = (value: unknown): value is tg.Location.Arg.Component => {
				return LocalComponent.is(value) || RemoteComponent.is(value);
			};
		}

		export type LocalComponent = {
			regions?: Array<string> | undefined;
		};

		export namespace LocalComponent {
			export let is = (
				value: unknown,
			): value is tg.Location.Arg.LocalComponent => {
				return (
					typeof value === "object" &&
					value !== null &&
					!Array.isArray(value) &&
					!("components" in value) &&
					!("name" in value) &&
					!("region" in value) &&
					!("remote" in value) &&
					("regions" in value
						? Array.isArray(value.regions) &&
							value.regions.every((region) => typeof region === "string")
						: true)
				);
			};
		}

		export type RemoteComponent = {
			regions?: Array<string> | undefined;
			name: string;
		};

		export namespace RemoteComponent {
			export let is = (
				value: unknown,
			): value is tg.Location.Arg.RemoteComponent => {
				return (
					typeof value === "object" &&
					value !== null &&
					!Array.isArray(value) &&
					!("components" in value) &&
					!("region" in value) &&
					"name" in value &&
					typeof value.name === "string" &&
					("regions" in value
						? Array.isArray(value.regions) &&
							value.regions.every((region) => typeof region === "string")
						: true)
				);
			};
		}

		export let is = (value: unknown): value is tg.Location.Arg => {
			return (
				typeof value === "object" &&
				value !== null &&
				!Array.isArray(value) &&
				"components" in value &&
				Array.isArray(value.components) &&
				value.components.every((component) => Component.is(component))
			);
		};

		export let toDataString = (value: tg.Location.Arg): string => {
			return value.components
				.map((component) => {
					let regions = component.regions?.filter(
						(region) => region.length > 0,
					);
					if ("name" in component) {
						let output = "remote";
						if (component.name !== "default") {
							output += `:${component.name}`;
						}
						if (regions !== undefined && regions.length > 0) {
							output += `(${regions.join(",")})`;
						}
						return output;
					}
					let output = "local";
					if (regions !== undefined && regions.length > 0) {
						output += `(${regions.join(",")})`;
					}
					return output;
				})
				.join(",");
		};

		export let fromDataString = (data: string): tg.Location.Arg => {
			let index = skipWhitespace(data, 0);
			let components: Array<tg.Location.Arg.Component> = [];
			while (index < data.length) {
				let component = parseComponent(data, index);
				components.push(component.value);
				index = skipWhitespace(data, component.index);
				if (index >= data.length) {
					break;
				}
				if (data[index] !== ",") {
					throw new Error("invalid location arg");
				}
				index = skipWhitespace(data, index + 1);
			}
			return { components };
		};

		export let fromLocation = (value: tg.Location): tg.Location.Arg => {
			if (!("name" in value)) {
				return {
					components: [
						{
							regions: value.region === undefined ? undefined : [value.region],
						},
					],
				};
			}
			return {
				components: [
					{
						name: value.name,
						regions: value.region === undefined ? undefined : [value.region],
					},
				],
			};
		};

		export let toLocation = (
			value: tg.Location.Arg | undefined,
		): tg.Location | undefined => {
			let component = value?.components[0];
			if (component === undefined || value?.components.length !== 1) {
				return undefined;
			}
			if (!("name" in component)) {
				let region = component.regions?.[0];
				if (component.regions !== undefined && component.regions.length !== 1) {
					return undefined;
				}
				return { region };
			}
			let region = component.regions?.[0];
			if (component.regions !== undefined && component.regions.length !== 1) {
				return undefined;
			}
			return {
				name: component.name,
				region,
			};
		};
	}
}

let parseComponent = (
	data: string,
	index: number,
): { value: tg.Location.Arg.Component; index: number } => {
	if (data.startsWith("local", index)) {
		let next = index + "local".length;
		let regions = parseOptionalRegions(data, next);
		return {
			value: regions.value === undefined ? {} : { regions: regions.value },
			index: regions.index,
		};
	}
	if (!data.startsWith("remote", index)) {
		throw new Error("invalid location arg");
	}
	let next = index + "remote".length;
	let name = "default";
	if (data[next] === ":") {
		let parsed = parseName(data, next + 1);
		name = parsed.value;
		next = parsed.index;
	}
	let regions = parseOptionalRegions(data, next);
	let value: tg.Location.Arg.RemoteComponent = { name };
	if (regions.value !== undefined) {
		value.regions = regions.value;
	}
	return { value, index: regions.index };
};

let parseOptionalRegions = (
	data: string,
	index: number,
): { value: Array<string> | undefined; index: number } => {
	index = skipWhitespace(data, index);
	if (data[index] !== "(") {
		return { value: undefined, index };
	}
	index += 1;
	let regions: Array<string> = [];
	while (true) {
		index = skipWhitespace(data, index);
		let parsed = parseName(data, index);
		regions.push(parsed.value);
		index = skipWhitespace(data, parsed.index);
		if (data[index] === ",") {
			index += 1;
			continue;
		}
		if (data[index] !== ")") {
			throw new Error("invalid location arg");
		}
		return { value: regions, index: index + 1 };
	}
};

let parseName = (
	data: string,
	index: number,
): { value: string; index: number } => {
	let next = index;
	while (next < data.length && isNameChar(data[next]!)) {
		next += 1;
	}
	if (next === index) {
		throw new Error("invalid location arg");
	}
	return {
		value: data.slice(index, next),
		index: next,
	};
};

let skipWhitespace = (data: string, index: number): number => {
	while (index < data.length && /\s/u.test(data[index]!)) {
		index += 1;
	}
	return index;
};

let isNameChar = (char: string): boolean => {
	return /^[A-Za-z0-9_-]$/u.test(char);
};
