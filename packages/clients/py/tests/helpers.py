"""A recording object store for client logic tests without a running server."""

import base64
import importlib
import unittest
from unittest.mock import patch

from tangram.referent import Referent


class ObjectClient:
    def __init__(self):
        self.objects = {}

    async def put_object(self, id, data, **options):
        self.objects[id] = data
        return {"object": Referent(id)}

    async def post_object_batch(self, objects, **options):
        for object_ in objects:
            self.objects[object_["id"]] = object_["data"]
        return {"objects": [Referent(object_["id"]) for object_ in objects]}

    async def get_object(self, id, **options):
        return {"data": self.objects[id], "tokens": {}, "children": {}}

    async def read(self, id, **options):
        def bytes_(id):
            value = self.objects[id]["value"]
            if "bytes" in value:
                return base64.b64decode(value["bytes"])
            return b"".join(bytes_(child["blob"]) for child in value["children"])

        data = bytes_(id)
        position = options.get("position", 0)
        length = options.get("length")
        return data[position:] if length is None else data[position : position + length]


class ObjectTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.object_client = ObjectClient()
        client_module = importlib.import_module("tangram.client")
        patcher = patch.object(client_module, "client", self.object_client)
        patcher.start()
        self.addCleanup(patcher.stop)
