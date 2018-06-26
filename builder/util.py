import warnings

class FromDictMixin():
    def __init__(self, *args, **kwargs):
        # apply json properties to existing attributes
        attributes = self.__dict__.keys()
        if args:
            if len(args) > 1:
                warnings.warn("Positional arguments after the first are ignored.")
            struct = args[0]
            for key in struct:
                if key in attributes:
                    setattr(self, key, self.load_attribute(key, struct[key]))
                else:
                    warnings.warn("JSON field {} ignored.".format(key))

        # override any json properties with the named ones
        for key in kwargs:
            if key in attributes:
                setattr(self, key, self.load_attribute(key, kwargs[key]))
            else:
                warnings.warn("Keyword argument {} ignored.".format(key))

    def load_attribute(self, key, value):
        return value

    def dump(self):
        prop_dict = vars(self)
        return recursive_dump(prop_dict)

def recursive_dump(value):
    # recursively call dump() for nested objects to generate a json-serializable dict
    if isinstance(value, dict):
        return {key:recursive_dump(value[key]) for key in value}
    elif isinstance(value, list):
        return [recursive_dump(v) for v in value]
    else:
        try:
            return value.dump()
        except AttributeError:
            return value
