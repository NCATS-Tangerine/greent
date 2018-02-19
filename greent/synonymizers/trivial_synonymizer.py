"""Not all types can be synonymized, either because we only care about a single identifier system, or because
we don't know how to cross the systems.  But we still need to call a synonymize function for these node types.
This trivial (no-op) gets called."""

def synonymize(node,gt):
    return