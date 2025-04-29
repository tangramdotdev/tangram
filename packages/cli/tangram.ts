import * as autobuild from "autobuild";
import * as std from "std";
import source from "." with { type: "directory" };
export default () => autobuild.build({ env: env(), source });
export const env = () => std.env(autobuild.env({ source }));
