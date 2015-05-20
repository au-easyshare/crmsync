# create a class for options from dicts so we can use options.argument
class MagicOptions:
    def __init__(self, *dicts):
        self.options = dict()
        if dicts:
            for cset in dicts:
                self.options.update(cset)

    def get(self, entry, val_if_not_present=None):
        return self.options.get(entry, val_if_not_present)

    def __getattr__(self, entry):
        return self.get(entry)

    def update(self, values_to_add):
        # TODO: this can be done in
        for kk, ii, in values_to_add.items():
            if ii is not None:
                self.options[kk] = ii
#        self.options.update(values_to_add)

    def as_dict(self):
        return self.options
