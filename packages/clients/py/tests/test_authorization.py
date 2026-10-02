"""Check proof inheritance without changing authorization scope."""

import base64
import json
import unittest

import tangram as tg
from tangram import authorization


def token(resource, permissions, key="test"):
    def encode(value):
        return base64.b64encode(json.dumps(value).encode()).decode()

    return ".".join(
        [
            "0",
            encode(
                {
                    "expires_at": 2**63 - 1,
                    "permissions": permissions,
                    "resource": resource,
                }
            ),
            encode({"key": key, "algorithm": "ed25519"}),
            "signature",
        ]
    )


class AuthorizationTests(unittest.TestCase):
    def test_scopes_and_opaque_tokens(self):
        node = token("fil_one", ["object_node"])
        subtree = token("fil_one", ["object_subtree"])
        unrelated = token("fil_two", ["object_node"])
        sync = token("syn_one", ["sync_read"])
        tokens = {
            "local": [node, subtree, unrelated, sync, "opaque", "opaque"],
            "remote": [node],
        }
        normalized = authorization.normalize(tokens)
        self.assertEqual(
            normalized["local"], sorted([subtree, unrelated, sync, "opaque"])
        )
        self.assertEqual(normalized["remote"], [node])
        scoped = authorization.normalize(tokens, "fil_one")
        self.assertEqual(scoped["local"], sorted([subtree, sync]))
        self.assertEqual(scoped["remote"], [node])

    def test_token_namespace_permissions(self):
        for granted, needed in (
            ("object_subtree", "object_node"),
            ("process_parent", "process_subtree_log_objects"),
            ("process_subtree_output_objects", "process_node_output_objects"),
            ("organization_admin", "organization_write"),
            ("group_write", "group_read"),
            ("sandbox_write", "sandbox_read"),
        ):
            proof = token("resource", [granted])
            self.assertTrue(authorization.Token.authorizes(proof, "resource", needed))
            self.assertFalse(authorization.Token.authorizes(proof, "other", needed))
        self.assertFalse(
            authorization.authorizes(
                token("resource", ["sandbox_admin"]), "resource", "sandbox_read"
            )
        )
        self.assertFalse(
            authorization.authorizes(
                token("resource", ["process_subtree"]),
                "resource",
                "process_node_output_objects",
            )
        )
        self.assertIsNone(authorization.Token.resource("opaque"))
        self.assertEqual(
            authorization.Token.resource(token("resource", [])), "resource"
        )

    def test_tokens_namespace_and_mutation(self):
        proof = token("fil_one", ["object_node"])
        original = {"local": [proof], "remote": []}
        cloned = authorization.Tokens.clone(original)
        self.assertIsNot(cloned["local"], original["local"])
        self.assertEqual(authorization.Tokens.clone(None), {})
        self.assertTrue(authorization.Tokens.is_empty({"local": []}))
        self.assertIs(authorization.Tokens.local(original), original["local"])
        self.assertIsNone(authorization.Tokens.local({}))
        self.assertEqual(authorization.Tokens.with_local([]), {})
        self.assertEqual(
            authorization.Tokens.with_local([proof, proof]), {"local": [proof]}
        )
        self.assertIsNone(authorization.Tokens.normalize(original))
        self.assertEqual(original, {"local": [proof]})
        parent = {"remote": [proof]}
        self.assertIsNone(authorization.Tokens.inherit(original, parent))
        self.assertEqual(original, {"local": [proof], "remote": [proof]})
        self.assertIsNot(original["remote"], parent["remote"])

    def test_expiration_parser_matches_js_integer_syntax(self):
        def raw(body):
            return ".".join(
                [
                    "0",
                    base64.b64encode(body.encode()).decode(),
                    base64.b64encode(b'{"algorithm":"ed25519","key":"test"}').decode(),
                    "signature",
                ]
            )

        for expiration in ("true", '"1"', "1.0", "1e2", str(2**63), str(-(2**63) - 1)):
            proof = raw(
                '{"resource":"fil_one","permissions":[],"expires_at":'
                + expiration
                + "}"
            )
            self.assertIsNone(authorization.parse(proof), expiration)
        duplicate = raw(
            '{"resource":"fil_one","permissions":[],"expires_at":1,"expires_at":2}'
        )
        self.assertIsNone(authorization.parse(duplicate))
        for expiration in (str(-(2**63)), str(2**63 - 1)):
            proof = raw(
                '{"resource":"fil_one","permissions":[],"expires_at":'
                + expiration
                + "}"
            )
            self.assertEqual(authorization.parse(proof)["expires_at"], int(expiration))

    def test_inheritance_does_not_mutate_children(self):
        child = tg.File("hello")
        child.tokens = {"local": [token(child.id, ["object_node"])]}
        parent = tg.Directory({"hello": child})
        self.assertEqual(parent.to_referent().options["tokens"], child.tokens)
        self.assertEqual(parent.tokens, {})
        self.assertEqual(len(child.tokens["local"]), 1)

    def test_locations_stay_independent(self):
        proof = token("fil_one", ["object_node"])
        self.assertEqual(
            authorization.inherit({"local": [proof]}, {"remote": [proof]}),
            {"local": [proof], "remote": [proof]},
        )


if __name__ == "__main__":
    unittest.main()
