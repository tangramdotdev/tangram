export type Location = Location.Local | Location.Remote;

export namespace Location {
	export type Local = {
		regions?: Array<string> | undefined;
	};

	export namespace Local {
		export let is = (value: unknown): value is Location.Local => {
			return (
				typeof value === "object" &&
				value !== null &&
				!Array.isArray(value) &&
				!("remote" in value)
			);
		};
	}

	export type Remote = {
		regions?: Array<string> | undefined;
		remote: string;
	};

	export namespace Remote {
		export let is = (value: unknown): value is Location.Remote => {
			return (
				typeof value === "object" &&
				value !== null &&
				!Array.isArray(value) &&
				"remote" in value &&
				typeof value.remote === "string"
			);
		};
	}

	export let is = (value: unknown): value is Location => {
		return Local.is(value) || Remote.is(value);
	};
}

export type Locations = {
	local?: boolean | Location.Local | undefined;
	remotes?: boolean | Array<Location.Remote> | undefined;
};

export namespace Locations {
	export let fromLocation = (
		location: Location | undefined,
	): Locations | undefined => {
		if (location === undefined) {
			return undefined;
		}
		if (Location.Local.is(location)) {
			return {
				local: location.regions === undefined ? true : location,
				remotes: false,
			};
		}
		return {
			local: false,
			remotes: [location],
		};
	};

	export let toLocation = (
		locations: Locations | undefined,
	): Location | undefined => {
		if (locations === undefined) {
			return undefined;
		}
		if (
			(locations.local === true || Location.Local.is(locations.local)) &&
			(locations.remotes === undefined || locations.remotes === false)
		) {
			return locations.local === true ? {} : locations.local;
		}
		if (
			(locations.local === undefined || locations.local === false) &&
			Array.isArray(locations.remotes) &&
			locations.remotes.length === 1
		) {
			return locations.remotes[0];
		}
		return undefined;
	};
}
