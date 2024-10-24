export namespace path {
	export type Component =
		| Component.Current
		| Component.Normal
		| Component.Parent
		| Component.Root;

	export namespace Component {
		export type Current = ".";

		export let Current = ".";

		export type Normal = string;

		export type Parent = "..";

		export let Parent = "..";

		export type Root = "/";

		export let Root = "/";

		export let isNormal = (component: Component): component is Normal => {
			return (
				component !== Current && component !== Parent && component !== Root
			);
		};
	}

	export let components = (pathArg: string): Array<Component> => {
		let subComponents = pathArg.split("/");
		if ((subComponents.at(-1) as string).length === 0) {
			subComponents.pop();
		}
		return pathArg.startsWith("/")
			? [Component.Root, ...subComponents.slice(1)]
			: subComponents;
	};

	export let fromComponents = (components: Array<Component>): string => {
		if (components.length === 0) {
			return "";
		} else if (components[0] === Component.Root) {
			return `/${components.slice(1).join("/")}`;
		} else {
			return components.join("/");
		}
	};

	export let isAbsolute = (pathArg: string): boolean => {
		if (pathArg.length === 0) {
			return false;
		}
		let component = path.components(pathArg)[0];
		return component === Component.Root;
	};

	export let join = (...args: Array<string | undefined>): string => {
		let components: Array<string> = [];
		for (let arg of args) {
			if (arg === undefined) {
				continue;
			}
			if (isAbsolute(arg)) {
				components = path.components(arg);
			} else {
				components = components.concat(path.components(arg));
			}
		}
		return path.fromComponents(components);
	};

	export let parent = (arg: string): string | undefined => {
		if (arg.length === 0) {
			throw new Error("expected a non-empty string");
		}
		let components = path.components(arg);
		if (components.length === 0) {
			return undefined;
		}
		return components.slice(0, -1).join("/");
	};
}
