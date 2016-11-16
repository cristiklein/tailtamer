#!/usr/bin/env python3
"""
Main module of the simulator. Processes input to simulation, steers simulation
and outputs results.
"""

import collections
import copy
import csv
import decimal
import itertools
import logging
import multiprocessing
import random
import sys
import time
import traceback

import simpy

# TODO: This module is getting long, we need to split it up.

Result = collections.namedtuple('Result', 'response_time')
TraceItem = collections.namedtuple('TraceItem', 'when who direction')

def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = itertools.tee(iterable) # pylint: disable=invalid-name
    next(b, None)
    return zip(a, b)

def pretty_kwds(kwds, sep=' '):
    "pretty_kwds(a=1, b=2) -> 'a=1 b=2'"
    return sep.join(sorted([str(k)+'='+str(v) for k, v in kwds.items()]))

def bounded_pareto(rng, alpha, L, H):
    U = rng.random()
    Ha = H**alpha
    La = L**alpha
    return (-(U*Ha - U*La - Ha)/(Ha * La)) ** (-1.0/alpha)

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

def _trace_request(instance_method):
    """
    Decorates an instance method to trace a request as it goes through the
    distributed systems.  This decorator makes the following assumptions about
    what it decorates:
    - The instance has an `_env` attribute that points to a simulation
      environment.
    - The method has a `request` attribute that points to request to be traced.
    """
    # pylint: disable=protected-access
    def wrapper(instance, request, *v, **k):
        """
        Wraps a SimPy-style generator-based process, marking entry and exit of a
        request.
        """
        request.do_trace(who=instance, when=instance._env.now,
                         direction='enter')
        yield from instance_method(instance, request, *v, **k)
        request.do_trace(who=instance, when=instance._env.now,
                         direction='exit')
    return wrapper

class Cancelled(Exception):
    """
    Represents the fact that a request was cancelled, as requested by the user.
    """
    pass

class RequestTiePair(object):
    """
    Represents the information necessary to produce request ties. Tying requests
    is a technique popularised by Google (Tail at Scale) to reduce tail response
    time. It consists in sending two tied requests: When one starts, the other
    one is cancelled. To avoid the two requests cancelling each other out,
    symmetry is broken by marking one request as high-priority, while the other
    one as low-priority. Once started, the high-priority request can no longer
    be cancelled, whereas the low-priority request can be cancelled after
    startal.
    """
    def __init__(self, env):
        self._start_event_high_prio = simpy.events.Event(env)
        self._start_event_low_prio = simpy.events.Event(env)

    @property
    def high_prio(self):
        """
        Returns a tuple necessary for the high-priority request:
        cancel_after_start, start_event, cancel_event. The work is supposed to
        trigger `start_event` on startal and cancel itself when `cancel_event`
        is triggered.
        """
        return False, self._start_event_high_prio, self._start_event_low_prio

    @property
    def low_prio(self):
        """
        Returns a tuple necessary for the high-priority request:
        cancel_after_start, start_event, cancel_event
        """
        return True, self._start_event_low_prio, self._start_event_high_prio

class Work(object):
    """
    Simulates work that has to be performed. Work must only be created by
    microservices and consumed by lowest-level executors.
    """
    def __init__(self, env, work, request, tie=None):
        assert work >= 0

        self._env = env
        self._initial = work
        self._remaining = work
        self._process = None
        self._cancelled = False
        self._request = request
        self._tie = tie

        if self._tie:
            cancel_after_start, start_event, cancel_event = self._tie
            cancel_event.callbacks.append(self.cancel)

    def consume(self, max_work_to_consume):
        """
        Consumes work, i.e., sleeps for a given maximum amount of time.  Work is
        currently equal to time, but may in future be modulated, e.g., due to
        frequency scaling. The method returns as soon as either all work is
        consumed or the maximum amount, given as parameter, is reached.
        """
        assert max_work_to_consume > 0
        assert self._process is None

        cancel_after_start, start_event, cancel_event = \
            self._tie if self._tie else (False, None, None)

        if self._cancelled:
            # cancelled before startal
            raise Cancelled()

        self._process = self._env.active_process
        work_to_consume = min(self._remaining, max_work_to_consume)
        assert work_to_consume > 0
        try:
            started_at = self._env.now
            if not cancel_after_start and cancel_event and \
                    cancel_event.callbacks:
                cancel_event.callbacks.remove(self.cancel)
            if start_event and not start_event.triggered:
                start_event.succeed()
            yield self._env.timeout(work_to_consume)
        except simpy.Interrupt as interrupt:
            if interrupt.cause=='cancelled':
                assert self._cancelled
                raise Cancelled() from interrupt
            raise
        finally:
            ended_at = self._env.now
            self._remaining -= (ended_at-started_at)
            self._process = None
            if cancel_event and cancel_event.callbacks:
                cancel_event.callbacks.remove(self.cancel)

    def cancel(self, _=None):
        """
        Cancel all outstanding work, if any is left.
        """
        if self.consumed or self._cancelled:
            return

        self._cancelled = True
        if self._process:
            self._process.interrupt(cause='cancelled')

    @property
    def cancelled(self):
        """
        True if the request was cancelled before completion.
        """
        return self._cancelled

    @property
    def amount_consumed(self):
        """
        Returns the amount of work consumed so far. We try not to return the
        amount of work remaining, to hide this information from the scheduler.
        """
        return self._initial-self._remaining

    @property
    def consumed(self):
        """
        Returns True if this work item was fully consumed, False otherwise.
        """
        return self._remaining == 0

class Thread(NamedObject):
    """
    Simulates a thread, i.e., something that the scheduler can use to decide
    what to execute next.
    """
    def __init__(self, prefix='thr', name=None):
        super().__init__(prefix=prefix, name=name)
        self.vruntime = 0

class VirtualMachine(NamedObject):
    """
    Simulates a virtual machine.
    """
    ALLOWED_SCHEDULERS = [
        'cfs',
        'fifo',
        'ps',
        'tt',
        'tt+p',
        'ttlas',
    ]

    def __init__(self, env, num_cpus, prefix='vm', name=None,
            context_switch_overhead=0):
        super().__init__(prefix=prefix, name=name)

        self._env = env
        self._cpus = simpy.PreemptiveResource(env, num_cpus)
        self._executor = None
        self.set_scheduler('ps', '0.005')

        self._context_switch_overhead = \
            self._env.to_time(context_switch_overhead)
        ## stores last work performed on each CPU, to decide whether context
        # switching overhead should be added or not.
        self._cpu_cache = [ None ] * num_cpus
        ## keep track of which CPUs are busy
        self._cpu_is_idle  = [ True ] * num_cpus
        ## conversion from VCPU to OS thread, à la type 2 hypervisor
        self._cpu_thread = [ Thread(name=str(self)+'_cpu'+str(i)) for i in range(num_cpus) ]

        self._cpu_time = 0

        self._num_active_cpus = 0

    def run_on(self, executor):
        """
        Sets the executor (usually a PhysicalMachine) on which this
        VirtualMachine should run. If not executor is given, then the
        VirtualMachine bahaves just like a physical one.
        """
        self._executor = executor

    def set_scheduler(self, scheduler, timeslice=None):
        """
        Sets a CPU scheduler and some scheduling parameters.
        """
        if scheduler not in self.ALLOWED_SCHEDULERS:
            raise ValueError('Invalid scheduler {0}. Allowed schedulers: {1}'
                             .format(scheduler, self.ALLOWED_SCHEDULERS))
        if timeslice is None:
            if scheduler in ['ps', 'tt', 'ttlas']:
                timeslice = '0.005'
            else:
                timeslice = 'inf'
        self._scheduler = (scheduler, self._env.to_time(timeslice))

    #@_trace_request
    def execute(self, sched_entity, request, work, max_work_to_consume='inf'):
        """
        Consume the given work up to the given limit. When taking scheduling
        decisions, take into account that the work item is related to the
        given request.
        """
        max_work_to_consume = self._env.to_time(max_work_to_consume)
        executor = self._executor
        scheduler, timeslice = self._scheduler
        cpus = self._cpus
        cpu_request = self._cpus.request
        num_cpus = self._cpus.capacity

        sched_latency = self._env.to_time('0.024')
        sched_min_granularity = self._env.to_time('0.003')

        # A smaller number means higher priority.
        if scheduler == 'cfs':
            preempt = False
            priority = 0
        elif scheduler == 'fifo':
            preempt = False
            priority = 0
        elif scheduler == 'ps':
            preempt = False
            priority = 0
        elif scheduler == 'tt':
            preempt = False
            priority = request.start_time
        elif scheduler == 'tt+p':
            preempt = True
            priority = request.start_time
        elif scheduler == 'ttlas':
            preempt = False
            priority = request.attained_time
        else:
            raise NotImplementedError() # should never get here

        while max_work_to_consume > 0 and not work.consumed:
            if scheduler == 'ttlas':
                priority = request.attained_time
            if scheduler == 'cfs':
                priority = sched_entity.vruntime

            # For priority "A smaller number means higher priority."
            with cpu_request(priority=priority, preempt=preempt) as req:
                try:
                    amount_consumed_before = work.amount_consumed
                    cpu_id = None

                    yield req

                    if scheduler == 'cfs':
                        timeslice = self._env.to_time(
                            max(sched_latency/(len(cpus.users)+len(cpus.queue)),
                                sched_min_granularity))
                    work_to_consume = min(timeslice, max_work_to_consume)

                    self._num_active_cpus += 1

                    assert self._num_active_cpus <= num_cpus, \
                        "Weird! Attempt to execute more requests "+\
                        "concurrently than available CPUs. There "+\
                        "is a bug in the simulator."

                    # This code is a bit cryptic to improve performance. In
                    # essence, we simulate CPU caches and track what work
                    # was execute on each CPU. If we find the CPU on which the
                    # current work was last executed, we consider the cache
                    # to be hot and no overhead is paid. Otherwise, we
                    # search for an idle CPU, consider the cache cold and
                    # pay the overhead.
                    cache_is_hot = False

                    some_idle_cpu_id = None
                    for cpu_id in range(len(self._cpu_is_idle)):
                        if self._cpu_cache[cpu_id] == work:
                            cache_is_hot = True
                            break
                        if self._cpu_is_idle[cpu_id]:
                            some_idle_cpu_id = cpu_id

                    if not cache_is_hot:
                        cpu_id = some_idle_cpu_id
                        self._cpu_cache[cpu_id] = work
                    self._cpu_is_idle[cpu_id] = False

                    if executor is None:
                        if not cache_is_hot:
                            yield self._env.timeout(self._context_switch_overhead)
                        yield from work.consume(work_to_consume)
                    else:
                        yield from executor.execute(self._cpu_thread[cpu_id], request, work,
                                                    work_to_consume)
                except simpy.Interrupt as interrupt:
                    if interrupt.cause.resource == cpus:
                        pass
                    else:
                        raise
                except Cancelled:
                    raise # propagate cancellation up
                finally:
                    work_consumed = work.amount_consumed-amount_consumed_before
                    self._cpu_time += work_consumed
                    max_work_to_consume -= work_consumed
                    self._num_active_cpus -= 1
                    sched_entity.vruntime += work_consumed
                    if executor is None:
                        request.add_attained_time(work_consumed)
                    if cpu_id is not None:
                        self._cpu_is_idle[cpu_id] = True

    @property
    def cpu_time(self):
        """
        Return the amont of CPU time (not including steal time) that the machine
        was active. This value is currently inaccurate if called during a
        timeslice.
        """
        return self._cpu_time

class PhysicalMachine(VirtualMachine):
    """
    Simulates a physical machine.
    """
    def __init__(self, *args, **kwargs):
        kwargs.update(prefix='pm')
        super().__init__(*args, **kwargs)
        super().set_scheduler('ps')

    # I'm not sure it makes sense to run PMs on something else. Maybe with
    # rack-scale computing?
    run_on = None

class Request(NamedObject):
    """
    Represents a request, travelling horizontally and vertically through the
    system. Only the client is allowed to create new requests.
    """
    def __init__(self, start_time):
        super().__init__(prefix='r')

        self._start_time = start_time
        self._end_time = None
        self._trace = []
        self._attained_time = 0

    @property
    def start_time(self):
        "The time the request entered the cloud application."
        return self._start_time

    def _get_end_time(self):
        "Get the time the request exited the cloud application."
        return self._end_time

    def _set_end_time(self, new_value):
        "Set the time the request exited the cloud application."
        assert self._end_time is None
        self._end_time = new_value

    end_time = property(_get_end_time, _set_end_time)

    def do_trace(self, when, who, direction):
        """
        Record a particular boundary event in the live of the request, such as
        entering or exiting a microservice, a VCPU or a CPU.
        """
        #print('{0:.6f} {1!s:<6} {2} {3}'.format(when, who, self, direction))
        self._trace.append(TraceItem(when=when, who=who, direction=direction))

    @property
    def trace(self):
        "Return the trace of this request."
        return self._trace

    @property
    def attained_time(self):
        "Return the amount of time this request has been processed so far."
        return self._attained_time

    def add_attained_time(self, by):
        self._attained_time += by

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
        if type(microservices) is not list:
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

        if type(microservices) is not list:
            microservices = [microservices]

        self._downstream.append((microservices, degree))

    @_trace_request
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
                r1 = self._env.process(microservice.on_request(request,
                    tie_pair.high_prio))
                r2 = self._env.process(secondary.on_request(request,
                    tie_pair.low_prio))

                try:
                    yield r1 | r2
                    if r2.triggered and r2.ok:
                        # Make r1 always the completed request
                        r1, r2 = r2, r1
                    r2.defused = True
                except Cancelled:
                    if not r2.triggered or r2.ok:
                        # Make r1 always the completed request
                        r1, r2 = r2, r1
                    yield r1
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
        # TODO: Associate a work item with the request that created it.
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

def assert_equal(actual, expected, message):
    """
    Asserts that two objects are equal.
    """
    if not (actual == expected):
        raise AssertionError('{0}: actual {1}, expected {2}'.format(message,
            actual, expected))

QUANTIZER = decimal.Decimal('1.000000000')

class NsSimPyEnvironment(simpy.Environment):
    """
    A simulation environment that represents time in Decimal with nanoseconds
    precision. Avoids all kind of funny floating-point artimetic issues.
    """
    def __init__(self):
        super().__init__(initial_time=self.to_time(0))

    def to_time(self, time):
        time = decimal.Decimal(time)
        if time.is_finite():
            time = time.quantize(QUANTIZER, rounding=decimal.ROUND_UP)
        return time

    def is_valid_time(self, time):
        return QUANTIZER.same_quantum(time)

    def timeout(self, delay, value=None):
        if not QUANTIZER.same_quantum(delay):
            raise RuntimeError(
                'Sub-nanosecond time injected into simulator: {}'.format(delay))
        return simpy.Environment.timeout(self, delay, value)

Layer = collections.namedtuple(
    'Layer', 'average_work relative_variance degree multiplicity '+
    'use_tied_requests')

DEFAULT_LAYERS_CONFIG = (
    Layer(average_work=0.001, relative_variance=0, degree=1, multiplicity=1,
        use_tied_requests=False),
    Layer(average_work=0.001, relative_variance=0, degree=1, multiplicity=1,
        use_tied_requests=False),
    Layer(average_work=0.010, relative_variance=0, degree=1, multiplicity=1,
        use_tied_requests=False),
    Layer(average_work=0.088, relative_variance=0, degree=1, multiplicity=1,
        use_tied_requests=False),
)

def layered_microservices(env, seed, simulation_duration, arrival_rate,
        layers_config, **kwds):
    """
    Produces a layered micro-service architecture and a single client.
    """
    clients = [
        OpenLoopClient(env, seed=seed,
                       arrival_rate=arrival_rate, until=simulation_duration),
    ]

    microservices = []
    layers = []
    for c in layers_config: # pylint: disable=invalid-name
        layer = []
        for _ in range(c.multiplicity):
            microservice = MicroService(
                env, seed=seed,
                name='l{0}us{1}'.format(len(layers), len(layer)),
                average_work=c.average_work,
                variance=c.average_work*c.relative_variance,
                degree=c.degree,
                use_tied_requests=c.use_tied_requests,
            )
            microservices.append(microservice)
            layer.append(microservice)
        layers.append(layer)

    #
    # Horizontal wiring
    #

    # Connect layer n-1 to all micro-services in layer n
    for caller_layer, callee_layer in pairwise([clients] + layers):
        for caller_microservice in caller_layer:
            for callee_microservice in callee_layer:
                caller_microservice.connect_to(callee_microservice)

    return clients, microservices

def so_microservices(env, seed, simulation_duration, arrival_rate,
        relative_variance=0.1, **kwds):
    """
    Produces a micro-service architecture similar to StackOverflow:
    https://nickcraver.com/blog/2016/02/17/stack-overflow-the-architecture-2016-edition/
    """
    clients = [
        OpenLoopClient(env, seed=seed,
                       arrival_rate=arrival_rate/4, until=simulation_duration),
        OpenLoopClient(env, seed=seed+1,
                       arrival_rate=arrival_rate/4, until=simulation_duration),
        OpenLoopClient(env, seed=seed+2,
                       arrival_rate=arrival_rate/4, until=simulation_duration),
        OpenLoopClient(env, seed=seed+3,
                       arrival_rate=arrival_rate/4, until=simulation_duration),
    ]


    haproxy = [
        MicroService(env, seed=seed, name='haproxy'+str(i),
            average_work=0.000001, relative_variance=relative_variance)
        for i in range(4)
    ]
    web = [
        MicroService(env, seed=seed, name='web'+str(i),
            average_work=0.0242, relative_variance=relative_variance)
        for i in range(4)
    ]
    redis = [
        MicroService(env, seed=seed, name='redis'+str(i),
            average_work=0.00000178, relative_variance=relative_variance)
        for i in range(2)
    ]
    search = [
        MicroService(env, seed=seed, name='search'+str(i),
            average_work=0.00541, relative_variance=relative_variance)
        for i in range(3)
    ]
    tag = [
        MicroService(env, seed=seed, name='tag'+str(i),
            average_work=0.0249, relative_variance=relative_variance)
        for i in range(3)
    ]
    db = [
        MicroService(env, seed=seed, name='db'+str(i),
            average_work=0.0012, relative_variance=relative_variance)
        for i in range(4)
    ]

    microservices = haproxy + web + redis + search + tag + db

    #
    # Horizontal wiring
    #
    for i in range(4):
        clients[i].connect_to(haproxy[i])

    for us in haproxy:
        us.connect_to(web, 0.31)

    for us in web:
        us.connect_to(redis, 28)
        us.connect_to(tag, 0.017)
        us.connect_to(search, 0.07)
        us.connect_to(db, 2)

    for us in tag:
        us.connect_to(db, 1)

    return clients, microservices

def run_simulation(
        method,
        method_param=None,
        arrival_rate=152,
        context_switch_overhead=0,
        layers_config=DEFAULT_LAYERS_CONFIG,
        software_layer_generator=layered_microservices,
        num_physical_machines=1,
        num_physical_cpus=16,
        simulation_duration=100,
        seed=1,
    ):
    """
    Wire the simulation entities together, run one simulation and collect
    results.
    """

    #
    # Simulation environment
    #
    env = NsSimPyEnvironment()

    #
    # Infrastructure layer
    #
    physical_machines = [
        PhysicalMachine(
            env, num_cpus=num_physical_cpus,
            context_switch_overhead=context_switch_overhead)
        for _ in range(num_physical_machines)
    ]

    #
    # Software layer
    #
    clients, microservices = software_layer_generator(**locals())

    #
    # Vertical wiring
    #
    virtual_machines = []
    vm_id = 0
    for microservice in microservices:
        virtual_machine = VirtualMachine(env, num_cpus=16)
        virtual_machine.name = 'vm_' + str(microservice)
        microservice.run_on(virtual_machine)
        # Round-robin VM to PM mapping
        virtual_machine.run_on(physical_machines[vm_id %
            num_physical_machines])
        vm_id += 1
        virtual_machines.append(virtual_machine)

    #
    # Configure schedulers
    #
    for physical_machine in physical_machines:
        physical_machine.set_scheduler(method, method_param)
    for virtual_machine in virtual_machines:
        virtual_machine.set_scheduler(method, method_param)

    #
    # Run simulation
    #
    try:
        env.run()
    except:
        print("Exception in simulator:", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        raise

    #
    # Collect data
    #
    response_times = sum([client.response_times for client in clients], [])

    #
    # Sanity check
    #
    expected_cpu_time = sum([
        us.total_work for us in microservices])

    # Ensure VMs did not produce more CPU that requests could have consumed
    actual_vm_cpu_time = sum([vm.cpu_time for vm in virtual_machines])
    assert_equal(actual_vm_cpu_time, expected_cpu_time,
                 'VM CPU time check failed')

    # Same for PMs
    actual_pm_cpu_time = sum([pm.cpu_time for pm in physical_machines])
    assert_equal(actual_pm_cpu_time, expected_cpu_time,
                 'PM CPU time check failed')

    # Same for requests
    total_attained_time = sum(client.total_attained_time for client in clients)
    assert_equal(actual_pm_cpu_time, total_attained_time,
                 'Total attained time check failed')

    wasted_cpu_time = sum([
        us.total_work_wasted for us in microservices])

    num_cancelled_requests = sum([
        us.num_cancelled_requests for us in microservices])

    return [
        Result(
            response_time=response_time,
        )
        for response_time in response_times]

def explore_param(output_filename, name, values, output_name=None,
        output_values=None, **simulation_kwds):
    """
    Runs several simulations for all scheduling methods, varying the given
    parameter along the given values, writing the results to the given output
    filename.
    """
    if output_name is None:
        output_name = name
    if output_values is None:
        output_values = values

    logger = logging.getLogger('tailtamer')
    logger.info("Exploring %s for %s", output_name,
                ' '.join([str(value) for value in output_values]))

    method_param_tie_tuples = [
        ('cfs' , None   , False), # pylint: disable=bad-whitespace
        ('cfs' , None   , True ), # pylint: disable=bad-whitespace
        ('fifo', None   , False), # pylint: disable=bad-whitespace
        ('tt'  , '0.005', False), # pylint: disable=bad-whitespace
        ('tt'  , '0.020', False), # pylint: disable=bad-whitespace
        ('tt+p', None   , False), # pylint: disable=bad-whitespace
        # Disabled for now: gives really bad performance
        #('ttlas', '0.001', False), # pylint: disable=bad-whitespace
    ]

    workers = multiprocessing.Pool() # pylint: disable=no-member
    futures = []
    for method, method_param, use_tied_requests in method_param_tie_tuples:
        for value, output_value in zip(values, output_values):
            kwds = dict(method=method, method_param=method_param)
            kwds.update(simulation_kwds)
            kwds[name] = value
            if use_tied_requests:
                if name is 'layers_config':
                    kwds['layers_config'] = with_tied_requests(value)
                else:
                    kwds['layers_config'] = \
                        with_tied_requests(DEFAULT_LAYERS_CONFIG)

            future = workers.apply_async(
                run_simulation,
                kwds=kwds)
            future.kwds = dict(kwds)
            future.kwds['method'] = \
                method + ('_' + str(method_param) if method_param else '') + \
                ('+tie' if use_tied_requests else '')
            if 'layers_config' in future.kwds:
                del future.kwds['layers_config']
            future.kwds[output_name] = output_value
            futures.append(future)

    with open(output_filename, 'w') as output_file:
        fieldnames = sorted(list(future.kwds.keys()))
        fieldnames += Result._fields # pylint: disable=protected-access
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()

        for future in futures:
            results = future.get()
            logger.info('Simulation completed: %s', pretty_kwds(future.kwds))
            for result in results:
                row = result._asdict() # pylint: disable=protected-access
                row.update(future.kwds)
                writer.writerow(row)

def with_relative_variance(template_layers_config, relative_variance):
    layers_config = [
        layer_config._replace(relative_variance=relative_variance) for
        layer_config in template_layers_config ]
    return layers_config

def with_last_degree(template_layers_config, degree):
    assert len(template_layers_config) >= 2
    layers_config = [
        layer_config for
        layer_config in template_layers_config ]
    layers_config[-1] = \
        layers_config[-1]._replace(average_work=layers_config[-1].average_work/degree)
    layers_config[-2] = \
        layers_config[-2]._replace(degree=degree)
    return layers_config

def with_last_multiplicity(template_layers_config, multiplicity):
    layers_config = [
        layer_config for
        layer_config in template_layers_config ]
    layers_config[-1] = \
        layers_config[-1]._replace(
            average_work=layers_config[-1].average_work/multiplicity,
            multiplicity=multiplicity)
    return layers_config

def with_tied_requests(template_layers_config):
    layers_config = [
        layer_config for
        layer_config in template_layers_config ]
    layers_config[-2] = \
        layers_config[-2]._replace(
            use_tied_requests=True)
    return layers_config

def main():
    """
    Entry-point for simulator.
    Simulate the system for each method and output results.
    """

    logger = logging.getLogger('tailtamer')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    channel = logging.StreamHandler()
    channel.setFormatter(formatter)
    logger.addHandler(channel)

    started_at = time.time()
    logger.info('Starting simulations')

    loads = [ 0.8, 0.825, 0.85, 0.875, 0.9, 0.925, 0.95, 0.975 ]
    explore_param('results-ar.csv', 'arrival_rate',
        [load*160.0 for load in loads],
        output_name='load',
        output_values=loads)
    
    # Variance
    explore_param('results-var.csv', 'layers_config',
        [
            with_relative_variance(DEFAULT_LAYERS_CONFIG, 0.00),
            with_relative_variance(DEFAULT_LAYERS_CONFIG, 0.05),
            with_relative_variance(DEFAULT_LAYERS_CONFIG, 0.10),
            with_relative_variance(DEFAULT_LAYERS_CONFIG, 0.20),
        ],
        output_name='relative_variance',
        output_values=[0.00, 0.05, 0.10, 0.20])

    # Degree
    explore_param('results-deg.csv', 'layers_config',
        [
            with_last_degree(DEFAULT_LAYERS_CONFIG, 1),
            with_last_degree(DEFAULT_LAYERS_CONFIG, 2),
            with_last_degree(DEFAULT_LAYERS_CONFIG, 5),
            with_last_degree(DEFAULT_LAYERS_CONFIG, 10),
        ],
        output_name='degree',
        output_values=[1, 2, 5, 10])

    # Multiplicity
    explore_param('results-mul.csv', 'layers_config',
        [
            with_last_multiplicity(DEFAULT_LAYERS_CONFIG, 1),
            with_last_multiplicity(DEFAULT_LAYERS_CONFIG, 2),
            with_last_multiplicity(DEFAULT_LAYERS_CONFIG, 5),
            with_last_multiplicity(DEFAULT_LAYERS_CONFIG, 10),
        ],
        output_name='multiplicity',
        output_values=[1, 2, 5, 10])

    # Context-switch overhead
    explore_param('results-ctx.csv', 'context_switch_overhead',
        ['0', '0.000001', '0.000010', '0.000100'])

    # Stackoverflow Architecture
    explore_param('results-so.csv',
        'num_physical_cpus', [21, 22, 23, 24],
        arrival_rate=2423,
        context_switch_overhead='0.000001',
        software_layer_generator=so_microservices,
        simulation_duration=10)

    ended_at = time.time()
    logger.info('Simulations completed in %f seconds', ended_at-started_at)

if __name__ == "__main__":
    main()
