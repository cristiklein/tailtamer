"""
Contains implementations for various client or load generators. Currently only
OpenLoopClient is implemented.
"""

import random

from .request import Request

class OpenLoopClient(object):
    """
    Simulates open-loop clients, with a given arrival rate.
    """
    def __init__(self, env, arrival_rate, until=None, seed=1):
        self._arrival_rate = arrival_rate
        self._env = env
        self._downstream = None
        self._requests = []
        self._until = until
        self._random = random.Random()
        self._random.seed(seed)

        self._env.process(self.run())

    def connect_to(self, microservices):
        "Sets the frontend microservice(s)."
        if not isinstance(microservices, list):
            self._downstream = [microservices]
        else:
            self._downstream = microservices

    def run(self):
        "Main process method, that issues requests."
        while self._until is None \
                or self._env.now < self._until:
            self._env.process(self._on_arrival())
            float_waiting_time = self._random.expovariate(self._arrival_rate)
            waiting_time = self._env.to_time(float_waiting_time)
            yield self._env.timeout(waiting_time)

    def _on_arrival(self):
        """
        Creates a new request, sends it to the frontend and records its
        response time.
        """
        request = Request(start_time=self._env.now)
        if len(self._downstream) > 1:
            microservice = self._random.choice(self._downstream)
        else:
            microservice = self._downstream[0]
        yield self._env.process(microservice.on_request(request))
        request.end_time = self._env.now
        self._requests.append(request)

    @property
    def response_times(self):
        "Returns all measured response times."
        return [r.end_time - r.start_time for r in self._requests]

    @property
    def total_attained_time(self):
        "Returns the total attained time for all requests."
        return sum(r.attained_time for r in self._requests)
