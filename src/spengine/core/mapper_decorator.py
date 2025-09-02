import functools
import jmespath

from spengine.model.mapper_context import MapperValueContext


def map_context(map):
    @functools.wraps(map)
    def wrap(self, data: dict, ctx: MapperValueContext | None):
        raw = data
        if ctx is not None:
            for c in ctx.ctx:
                raw[c.to] = jmespath.search(c.src, ctx.data)

        return map(self, raw, ctx)

    return wrap
