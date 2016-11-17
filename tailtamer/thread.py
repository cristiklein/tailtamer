"""
Currently contains just Thread
"""

from .base import NamedObject

class Thread(NamedObject):
    """
    Simulates a thread, i.e., something that the scheduler can use to decide
    what to execute next.
    """
    def __init__(self, prefix='thr', name=None):
        super().__init__(prefix=prefix, name=name)
        self.vruntime = 0

