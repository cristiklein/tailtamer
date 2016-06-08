#!/usr/bin/env python3
"""
Main module of the simulator. Processes input to simulation, steers simulation
and outputs results.
"""

import collections
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

def to_decimal_with_ns_prec(number):
    "Returns a Decimal with nano-second precision"
    return decimal.Context(prec=9, rounding=decimal.ROUND_DOWN).\
        create_decimal(number)

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

class Work(object):
    """
    Simulates work that has to be performed. Work must only be created by
    microservices and consumed by lowest-level executors.
    """
    def __init__(self, env, work):
        assert work >= 0

        self._env = env
        self._initial = work
        self._remaining = work
        self._process = None

    def consume(self, max_work_to_consume):
        """
        Consumes work, i.e., sleeps for a given maximum amount of time.  Work is
        currently equal to time, but may in future be modulated, e.g., due to
        frequency scaling. The method returns as soon as either all work is
        consumed or the maximum amount, given as parameter, is reached.
        """
        assert max_work_to_consume > 0
        assert self._process is None

        self._process = self._env.active_process
        work_to_consume = min(self._remaining, max_work_to_consume)
        assert work_to_consume > 0
        try:
            started_at = self._env.now
            yield self._env.timeout(work_to_consume)
        except simpy.Interrupt:
            raise
        finally:
            ended_at = self._env.now
            self._remaining -= (ended_at-started_at)
            self._process = None

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


class VirtualMachine(NamedObject):
    """
    Simulates a virtual machine.
    """
    ALLOWED_SCHEDULERS = [
        'ps',
        'tt',
        'tt+p',
    ]

    def __init__(self, env, num_cpus, prefix='vm', name=None):
        super().__init__(prefix=prefix, name=name)

        self._env = env
        self._cpus = simpy.PreemptiveResource(env, num_cpus)
        self._scheduler = ('ps', 0.005)
        self._executor = None

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
            if scheduler in ['ps', 'tt']:
                timeslice = '0.005'
            else:
                timeslice = 'inf'
        self._scheduler = (scheduler, self._env.to_time(timeslice))

    #@_trace_request
    def execute(self, request, work, max_work_to_consume='inf'):
        """
        Consume the given work up to the given limit. When taking scheduling
        decisions, take into account that the work item is related to the
        given request.
        """
        max_work_to_consume = self._env.to_time(max_work_to_consume)
        executor = self._executor
        scheduler, timeslice = self._scheduler
        cpus = self._cpus

        if scheduler == 'ps':
            preempt = False
            priority = 0
        elif scheduler == 'tt':
            preempt = False
            priority = request.start_time
        elif scheduler == 'tt+p':
            preempt = True
            priority = request.start_time
        else:
            raise NotImplementedError() # should never get here

        while max_work_to_consume > 0 and not work.consumed:
            with cpus.request(priority=priority, preempt=preempt) as req:
                work_to_consume = min(timeslice, max_work_to_consume)

                try:
                    amount_consumed_before = work.amount_consumed

                    yield req
                    self._num_active_cpus += 1
                    assert self._num_active_cpus <= cpus.capacity, \
                        "Weird! Attempt to execute more requests "+\
                        "concurrently than available CPUs. There "+\
                        "is a bug in the simulator."

                    if executor is None:
                        yield from work.consume(work_to_consume)
                    else:
                        yield from executor.execute(request, work,
                                                    work_to_consume)
                except simpy.Interrupt as interrupt:
                    if interrupt.cause.resource == cpus:
                        pass
                    else:
                        raise
                finally:
                    work_consumed = work.amount_consumed-amount_consumed_before
                    self._cpu_time += work_consumed
                    max_work_to_consume -= work_consumed
                    self._num_active_cpus -= 1

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

class OpenLoopClient(object):
    """
    Simulates open-loop clients, with a given arrival rate.
    """
    def __init__(self, env, arrival_rate, until=None, seed=1):
        self._arrival_rate = arrival_rate
        self._env = env
        self._downstream_microservice = None
        self._requests = []
        self._until = until
        self._random = random.Random()
        self._random.seed(seed)

        self._env.process(self.run())

    def connect_to(self, microservice):
        "Sets the frontend microservice."
        self._downstream_microservice = microservice

    def run(self):
        "Main process method, that issues requests."
        while self._until is None \
                or self._env.now < self._until:
            self._env.process(self._on_arrival())
            float_waiting_time = \
                max(self._random.expovariate(self._arrival_rate), 0)
            waiting_time = self._env.to_time(float_waiting_time)
            yield self._env.timeout(waiting_time)

    def _on_arrival(self):
        """
        Creates a new request, sends it to the frontend and records its
        response time.
        """
        request = Request(start_time=self._env.now)
        yield self._env.process(
            self._downstream_microservice.on_request(request))
        request.end_time = self._env.now
        self._requests.append(request)

    @property
    def response_times(self):
        "Returns all measured response times."
        return [r.end_time - r.start_time for r in self._requests]

class MicroService(NamedObject):
    """
    Simulates a micro-service, with a given average work and downcall structure.
    Currently, the execution model assumes one thread is created for each
    request.
    """
    def __init__(self, env, name, average_work, seed='', degree=1, variance=0):
        super().__init__(prefix='µs', name=name)

        self._env = env
        self._average_work = average_work
        self._variance = variance
        self._executor = None
        self._downstream_microservices = []
        self._degree = degree
        self._random = random.Random()
        self._random.seed(str(self)+str(seed))

        self._total_work = 0

    def run_on(self, executor):
        """
        Sets the executor (usually a VirtualMachine) on which this
        MicroService runs.
        """
        self._executor = executor

    def connect_to(self, microservice):
        "Add a downstream microservice to be called for each request."
        self._downstream_microservices.append(microservice)

    @_trace_request
    def on_request(self, request):
        """
        Handles a request; produces work, calls the underlying executor and
        calls downstream microservice.
        """
        float_demand = self._random.normalvariate(self._average_work,
                                                  self._variance)
        demand = self._env.to_time(float_demand)
        demand_between_calls = \
            demand / (len(self._downstream_microservices)*self._degree+1)

        yield self._env.process(
            self._compute(request, demand_between_calls))
        for _ in range(self._degree):
            for microservice in self._downstream_microservices:
                yield self._env.process(microservice.on_request(request))
                yield self._env.process(
                    self._compute(request, demand_between_calls))

    def _compute(self, request, demand):
        """
        Produces work and wait for the executor to consume it.
        """
        work = Work(self._env, demand)
        yield self._env.process(self._executor.execute(request, work))
        assert work.consumed
        self._total_work += demand

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

def assert_equal(actual, expected, message):
    """
    Asserts that two objects are equal.
    """
    assert actual == expected, \
        '{0}: actual {1}, expected {2}'.format(message, actual, expected)

Layer = collections.namedtuple(
    'Layer', 'average_work relative_variance degree multiplicity')

DEFAULT_LAYER_CONFIGS = (
    Layer(average_work=0.001, relative_variance=0, degree=1, multiplicity=1),
    Layer(average_work=0.001, relative_variance=0, degree=1, multiplicity=1),
    Layer(average_work=0.010, relative_variance=0, degree=1, multiplicity=1),
    Layer(average_work=0.088, relative_variance=0, degree=1, multiplicity=1),
)

def run_simulation(
        method,
        method_param=None,
        arrival_rate=155,
        layer_configs=DEFAULT_LAYER_CONFIGS,
        physical_machines=1,
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
    env = simpy.Environment()
    env.to_time = to_decimal_with_ns_prec

    #
    # Infrastructure layer
    #
    physical_machines = [
        PhysicalMachine(env, num_cpus=16)
        for _ in range(physical_machines)
    ]

    #
    # Software layer
    #
    clients = [
        OpenLoopClient(env, seed=seed,
                       arrival_rate=arrival_rate, until=simulation_duration),
    ]

    layers = []
    for c in layer_configs: # pylint: disable=invalid-name
        layer = []
        for _ in range(c.multiplicity):
            microservice = MicroService(
                env, seed=seed,
                name='l{0}us{1}'.format(len(layers), len(layer)),
                average_work=c.average_work,
                variance=c.average_work*c.relative_variance,
                degree=c.degree,
            )
            layer.append(microservice)
        layers.append(layer)

    #
    # Horizontal wiring
    #

    # Connect layer n-1 to all micro-services in layer n
    for caller_layer, callee_layer in pairwise(clients + layers):
        for caller_microservice in caller_layer:
            for callee_microservice in callee_layer:
                caller_microservice.connect_to(callee_microservice)

    #
    # Vertical wiring
    #
    virtual_machines = []
    for layer in layers:
        for microservice in layer:
            virtual_machine = VirtualMachine(env, num_cpus=16)
            virtual_machine.name = 'vm_' + str(microservice)
            microservice.run_on(virtual_machine)
            # TODO: Optionally add a VM to PM mapping algorithm.
            virtual_machine.run_on(physical_machines[0])
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
        us.total_work for layer in layers for us in layer])

    # Ensure VMs did not produce more CPU that requests could have consumed
    actual_vm_cpu_time = sum([vm.cpu_time for vm in virtual_machines])
    assert_equal(actual_vm_cpu_time, expected_cpu_time,
                 'VM CPU time check failed')

    # Same for PMs
    actual_pm_cpu_time = sum([pm.cpu_time for pm in physical_machines])
    assert_equal(actual_pm_cpu_time, expected_cpu_time,
                 'PM CPU time check failed')

    return [
        Result(
            response_time=response_time,
        )
        for response_time in response_times]

def explore_param(output_filename, name, values):
    """
    Runs several simulations for all scheduling methods, varying the given
    parameter along the given values, writing the results to the given output
    filename.
    """
    logger = logging.getLogger('tailtamer')
    logger.info("Exploring %s for %s", name,
                ' '.join([str(value) for value in values]))

    method_param_tuples = [
        ('ps'  , '0.005'), # pylint: disable=bad-whitespace
        ('ps'  , 'Inf'  ), # pylint: disable=bad-whitespace
        ('tt'  , '0.005'), # pylint: disable=bad-whitespace
        ('tt'  , '0.020'), # pylint: disable=bad-whitespace
        ('tt+p', None   ), # pylint: disable=bad-whitespace
    ]

    workers = multiprocessing.Pool() # pylint: disable=no-member
    futures = []
    for method, param in method_param_tuples:
        for value in values:
            kwds = dict(method=method, method_param=param)
            kwds[name] = value
            future = workers.apply_async(
                run_simulation,
                kwds=kwds)
            future.kwds = dict(kwds)
            future.kwds['method'] = \
                method + ('_' + str(param) if param else '')
            futures.append(future)

    with open(output_filename, 'w') as output_file:
        fieldnames = list(kwds.keys())
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

    explore_param('results-ar.csv', 'arrival_rate', [140, 145, 150, 155])

    ended_at = time.time()
    logger.info('Simulations completed in %f seconds', ended_at-started_at)

if __name__ == "__main__":
    main()
