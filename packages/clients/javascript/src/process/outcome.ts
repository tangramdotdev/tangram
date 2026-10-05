import * as tg from "../index.ts";

export type Outcome = {
	error: tg.Error | null;
	exit: number;
	output?: tg.Value;
};

export namespace Outcome {
	export type Data = {
		error?: tg.Error.Data | string | null;
		exit: number;
		output?: tg.Value.Data;
	};

	export let fromData = (data: tg.Process.Outcome.Data): tg.Process.Outcome => {
		let outcome: Outcome = {
			error:
				data.error !== undefined && data.error !== null
					? typeof data.error === "string"
						? tg.Error.withReferent(
								tg.Referent.fromDataString(
									data.error,
									(id) => id as tg.Error.Id,
								),
							)
						: tg.Error.fromData(data.error)
					: null,
			exit: data.exit,
		};
		if ("output" in data) {
			outcome.output = tg.Value.fromData(data.output);
		}
		return outcome;
	};

	export let inheritLocation = (
		outcome: tg.Process.Outcome,
		location: tg.Location | null,
	): void => {
		if (outcome.error !== null) {
			tg.Object.inheritLocation(outcome.error, location);
		}
		if (outcome.output !== undefined) {
			tg.Value.inheritLocation(outcome.output, location);
		}
	};

	export let inheritTokens = (
		outcome: tg.Process.Outcome,
		tokens: tg.Authorization.Tokens,
	): void => {
		if (outcome.error !== null) {
			tg.Object.inheritTokens(outcome.error, tokens);
		}
		if (outcome.output !== undefined) {
			tg.Value.inheritTokens(outcome.output, tokens);
		}
	};

	export let toData = (value: Outcome): Data => {
		let outcome: Data = {
			exit: value.exit,
		};
		if (value.error !== null) {
			outcome.error = tg.Error.toDataOrId(value.error);
		}
		if (value.output !== undefined) {
			outcome.output = tg.Value.toData(value.output);
		}
		return outcome;
	};
}
