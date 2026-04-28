export namespace path {
	/** A path component. */
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

	/** Split a path into its components */
	export let components = (arg: string): Array<Component> => {
		let components = arg.split("/");
		if (components.at(0)?.length === 0) {
			components[0] = Component.Root;
		}
		components = components.filter((component, i) => {
			if (i > 0 && component === Component.Current) {
				return false;
			}
			if (component.length === 0) {
				return false;
			}
			return true;
		});
		return components;
	};

	/** Create a path from an array of path components. */
	export let fromComponents = (components: Array<Component>): string => {
		if (components[0] === Component.Root) {
			return `/${components.slice(1).join("/")}`;
		} else {
			return components.join("/");
		}
	};

	/** Return true if the path is absolute. */
	export let isAbsolute = (arg: string): boolean => {
		return arg.startsWith("/");
	};

	/** Join paths. */
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
		return fromComponents(components);
	};

	/** Return the path with its last component removed. */
	export let parent = (arg: string): string | undefined => {
		let components = path.components(arg);
		if (components.length === 0) {
			return undefined;
		}
		return components.slice(0, -1).join("/");
	};
}
