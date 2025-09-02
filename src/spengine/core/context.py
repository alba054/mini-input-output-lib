from __future__ import annotations


class Context:
    _ctx: dict = dict()

    def set(self, key, value):
        self._ctx[key] = value

    def get(self, key):
        return self._ctx.get(key)

    @staticmethod
    def new() -> Context:
        return Context()
