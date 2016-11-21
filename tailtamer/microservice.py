"""
Package to simulate a microservice.
"""

import random

from .base import NamedObject
from .request import trace_request, Cancelled, RequestTiePair, Work
from .thread import Thread

class MicroService(NamedObject):
    """
    Simulates a micro-service, with a given average work and downcall structure.
    Currently, the execution model assumes one thread is created for each
    request.
    """
    def __init__(self, env, name, average_work, seed='', degree=1, variance=0,
                 use_tied_requests=False, relative_variance=None):
        super().__init__(prefix='µs', name=name)

        if relative_variance is not None:
            variance = average_work * relative_variance

        self._env = env
        self._average_work = average_work
        self._variance = variance
        self._executor = None
        self._downstream = []
        self._degree = degree
        self._random = random.Random()
        self._random.seed(str(self)+str(seed))
        self._use_tied_requests = use_tied_requests
        self._num_cancelled_requests = 0

        self._total_work = 0
        self._total_work_wasted = 0
        self._thread_pool = []
        self._num_threads = 0

    def run_on(self, executor):
        """
        Sets the executor (usually a VirtualMachine) on which this
        MicroService runs.
        """
        self._executor = executor

    def connect_to(self, microservices, degree=None):
        """
        Add an instance or a list of downstream microservice to be called for
        each request, a given number of times (degree>=1) or with a given
        probability (degree<1).
        """
        if degree is None:
            degree = self._degree
        assert int(degree) == degree or degree < 1

        if not isinstance(microservices, list):
            microservices = [microservices]

        self._downstream.append((microservices, degree))

    @trace_request
    def on_request(self, request, tie=None):
        """
        Handles a request; produces work, calls the underlying executor and
        calls downstream microservice.
        """

        # We assume the thread pool is large enough, we do not focus on soft
        # resource contention in this simulation
        if not self._thread_pool:
            self._thread_pool.append(Thread(name=str(self)+'_thr'+str(self._num_threads)))
            self._num_threads += 1
        thread = self._thread_pool.pop()

        demand = max(
            self._random.normalvariate(self._average_work, self._variance), 0)

        # XXX: Supposed to favour LAS, but it does not. Maybe our application
        # model invalidates heavy-tailed demand in total.
        #demand = bounded_pareto(self._random, 1, self._average_work,
        #        self._average_work + self._variance + 0.0001)

        actual_calls = []
        for microservices, degree in self._downstream:
            if len(microservices) > 1:
                microservice, secondary = self._random.sample(microservices, 2)
            else:
                microservice, secondary = microservices[0], microservices[0]
            if degree >= 1:
                for _ in range(0, degree):
                    actual_calls.append((microservice, secondary))
            else:
                to_call = self._random.uniform(0, 1) < degree
                if to_call:
                    actual_calls.append((microservice, secondary))

        num_computations = len(actual_calls)+1
        demand_between_calls = self._env.to_time(demand / num_computations)

        yield self._env.process(
            self._compute(thread, request, demand_between_calls, tie))
        for microservice, secondary in actual_calls:
            if not self._use_tied_requests:
                yield self._env.process(microservice.on_request(request))
            else:
                tie_pair = RequestTiePair(self._env)
                request1 = self._env.process(
                    microservice.on_request(request, tie_pair.high_prio))
                request2 = self._env.process(
                    secondary.on_request(request, tie_pair.low_prio))

                try:
                    yield request1 | request2
                    if request2.triggered and request2.ok:
                        # Make request1 always the completed request
                        request1, request2 = request2, request1
                    request2.defused = True
                except Cancelled:
                    if not request2.triggered or request2.ok:
                        # Make request1 always the completed request
                        request1, request2 = request2, request1
                    yield request1
            yield self._env.process(
                self._compute(thread, request, demand_between_calls))

        # NOTE: In case we exit through an exception, the thread will not
        # be returned to the thread pool. This is intentional, as we assume a
        # real implementation would create a fresh thread instead of saving a
        # thread in an unknown state.
        self._thread_pool.append(thread)

    def _compute(self, thread, request, demand, tie=None):
        """
        Produces work and wait for the executor to consume it.
        """
        work = Work(self._env, demand, request, tie)

        try:
            before_work_consumed = work.amount_consumed
            yield self._env.process(self._executor.execute(thread, request, work))
            assert work.consumed
            self._total_work += demand
        except Cancelled:
            assert work.cancelled
            self._num_cancelled_requests += 1
            work_consumed = work.amount_consumed - before_work_consumed
            self._total_work += work_consumed
            self._total_work_wasted += work_consumed
            raise

    @property
    def total_work(self):
        """
        Returns the total amount of work produced by this microservice. This
        serves as a sanity check: The following three values should be equal:
        - the sum of work produced by all microservices;
        - the sum of work consumed by all virtual machines;
        - the sum of work consumed by all physical machines.
        """
        return self._total_work

    @property
    def total_work_wasted(self):
        """
        Returns the total amount of work wasted due to tied requests.
        """
        return self._total_work_wasted

    @property
    def num_cancelled_requests(self):
        """
        Returns the number of requests that were cancelled.
        """
        return self._num_cancelled_requests

