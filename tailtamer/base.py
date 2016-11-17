import collections
import itertools

Result = collections.namedtuple('Result', 'response_time')

class NamedObject(object):
    """
    Gives classes a more human-friendly string identification as retrieved
    through `str()`. Name may either be set automatically based on a prefix or
    set by user.
    """
    prefix_to_num = collections.defaultdict(itertools.count)

    def __init__(self, prefix='unnamed', name=None):
        if name is None:
            self._name = prefix + str(next(NamedObject.prefix_to_num[prefix]))
        else:
            self._name = name

    def get_name(self):
        "Get the current name (same as __str__)"
        return self._name

    def set_name(self, name):
        "Sets a new name"
        self._name = name

    name = property(get_name, set_name)

    def __str__(self):
        return self._name
