class Environment(object):
    """
    Represent the simulation environment.

    TODO, likely with SimPy
    """
    def __init__(self):
        import random as EnvironmentRandom # avoid accidental usage
        self._random = EnvironmentRandom.Random()
        # TODO: Seed should be a command-line parameter to allow sensitivity testing.
        self._random.seed(1)

    def run(self):
        """
        Runs the simulation, either to completion or up to a given time.
        """
        pass

    def spawn(self, function):
        """
        Schedule a method to be executed.
        """
        pass

    @property
    def now(self):
        """
        Returns the current simulator time.
        """
        pass

    @property
    def random(self):
        """
        Get a pseudo-random number generator. Prefer this method, instead of
        instantiating a `random` directly, to ensure the simulator is idempotent.
        """
        return self._random
