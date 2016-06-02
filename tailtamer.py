#!/usr/bin/env python3
"""
Main module of the simulator. Processes input to simulation, steers simulation
and outputs results.
"""

import collections
import csv
import itertools
import multiprocessing
import random
import sys
import traceback

import simpy

Result = collections.namedtuple('Result', 'arrival_rate method response_time')
TraceItem = collections.namedtuple('TraceItem', 'when who direction')

def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = itertools.tee(iterable) # pylint: disable=invalid-name
    next(b, None)
    return zip(a, b)

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
        return self._name

    def set_name(self, name):
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
        request.do_trace(who=instance, when=instance._env.now,
                         direction='enter')
        yield from instance_method(instance, request, *v, **k)
        request.do_trace(who=instance, when=instance._env.now,
                         direction='exit')
    return wrapper

class VirtualMachine(NamedObject):
    """
    Simulates a virtual machine.
    """
    ALLOWED_SCHEDULERS = [
        'fifo',
        'ps',
        'tt',
        'tt+p'
    ]

    def __init__(self, env, num_cpus, prefix='vm', name=None):
        super().__init__(prefix=prefix, name=name)

        self._env = env
        self._cpus = simpy.PreemptiveResource(env, num_cpus)
        self._scheduler = 'fifo'
        self._executor = None

        self._cpu_time = 0

        self._num_active_cpus = 0

    def run_on(self, executor):
        self._executor = executor

    def set_scheduler(self, scheduler):
        if scheduler not in self.ALLOWED_SCHEDULERS:
            raise ValueError('Invalid scheduler {0}. Allowed schedulers: {1}'
                             .format(scheduler, self.ALLOWED_SCHEDULERS))
        self._scheduler = scheduler

    @_trace_request
    def execute(self, request, work):
        remaining_work = work
        if self._scheduler == 'fifo':
            preempt = False
            priority = 0
        elif self._scheduler == 'ps':
            preempt = False
            priority = 0
        elif self._scheduler == 'tt':
            preempt = False
            priority = request.start_time
        elif self._scheduler == 'tt+p':
            preempt = True
            priority = request.start_time
        else:
            raise NotImplementedError() # should never get here

        while remaining_work > 0:
            with self._cpus.request(priority=priority, preempt=preempt) as req:
                yield req
                self._num_active_cpus += 1
                assert self._num_active_cpus <= self._cpus.capacity, \
                        "Weird! Attempt to execute more requests "+\
                        "concurrently than available CPUs. There "+\
                        "is a bug in the simulator."
                try:
                    if self._scheduler in \
                            ['ps', 'tt']:
                        timeslice = 0.005
                        work_to_do = min(timeslice, remaining_work)
                    else:
                        work_to_do = remaining_work
                    if self._executor is None:
                        yield self._env.timeout(work_to_do)
                    else:
                        yield self._env.process(
                            self._executor.execute(request, work_to_do))
                    remaining_work -= work_to_do
                    self._cpu_time += work_to_do
                except simpy.Interrupt as interrupt:
                    work_done = self._env.now - interrupt.cause.usage_since
                    remaining_work -= work_done
                    self._cpu_time += work_done
                self._num_active_cpus -= 1

    def _log(self, *args):
        print('{0:.6f}'.format(self._env.now), *args)

    @property
    def cpu_time(self):
        # TODO: Inaccurate if called during a timeslice
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
        return self._start_time

    def get_end_time(self):
        return self._end_time

    def set_end_time(self, new_value):
        assert self._end_time is None
        self._end_time = new_value

    end_time = property(get_end_time, set_end_time)

    def do_trace(self, when, who, direction):
        #print('{0:.6f} {1!s:<6} {2} {3}'.format(when, who, self, direction))
        self._trace.append(TraceItem(when=when, who=who, direction=direction))

    @property
    def trace(self):
        return self._trace

class OpenLoopClient(object):
    """
    Simulates open-loop clients, with a given arrival rate.
    """
    def __init__(self, env, arrival_rate, until=None):
        self._arrival_rate = arrival_rate
        self._env = env
        self._downstream_microservice = None
        self._requests = []
        self._until = until

        self._env.process(self.run())

    def connect_to(self, microservice):
        self._downstream_microservice = microservice

    def run(self):
        while self._until is None \
                or self._env.now < self._until:
            self._env.process(self._on_arrival())
            waiting_time = self._env.random.expovariate(self._arrival_rate)
            yield self._env.timeout(waiting_time)

    def _on_arrival(self):
        request = Request(start_time=self._env.now)
        yield self._env.process(
            self._downstream_microservice.on_request(request))
        request.end_time = self._env.now
        self._requests.append(request)
        #for when, who, direction in request.trace:
        #    print('{0:.6f} {1!s:<6} {2}'.format(when, who, direction))
        #print()

    @property
    def response_times(self):
        return [r.end_time - r.start_time for r in self._requests]

class MicroService(NamedObject):
    """
    Simulates a micro-service, with a given average work and downcall structure.
    Currently, the execution model assumes one thread is created for each
    request.
    """
    def __init__(self, env, name, average_work, degree=1, variance=None):
        super().__init__(prefix='µs', name=name)

        self._env = env
        self._average_work = average_work
        self._variance = variance or (self._average_work / 10)
        self._executor = None
        self._downstream_microservices = []
        self._degree = degree

        self._total_work = 0

    def run_on(self, executor):
        self._executor = executor

    def connect_to(self, microservice):
        self._downstream_microservices.append(microservice)

    @_trace_request
    def on_request(self, request):
        demand = self._env.random.normalvariate(self._average_work,
                                                self._variance)
        demand_between_calls = \
            demand / (len(self._downstream_microservices)*self._degree+1)

        yield self._env.process(
            self._compute(request, demand_between_calls))
        for degree in range(self._degree):
            for microservice in self._downstream_microservices:
                yield self._env.process(microservice.on_request(request))
                yield self._env.process(
                    self._compute(request, demand_between_calls))

    def _compute(self, request, demand):
        yield self._env.process(self._executor.execute(request, demand))
        self._total_work += demand

    @property
    def total_work(self):
        return self._total_work

def assert_almost_equal(actual, expected, message, precision=0.001):
    """
    Asserts that two floating point values are within a small difference.
    """
    assert abs(actual-expected) < precision, \
        '{0}: actual {1}, expected {2}'.format(message, actual, expected)

def run_simulation(arrival_rate, method, physical_machines=1, seed=1):
    """
    Wire the simulation entities together, run one simulation and collect
    results.
    """

    #
    # Simulation environment
    #
    env = simpy.Environment()
    env.random = random.Random()
    env.random.seed(1)

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
    client_layer = [OpenLoopClient(env, arrival_rate=arrival_rate, until=100)]
    frontend_layer = [MicroService(env, name='fe0', average_work=0.001)]
    caching_layer = [MicroService(env, name='ca0', average_work=0.001)]
    business_layer = [MicroService(env, name='bu0', average_work=0.010, degree=3)]
    persistence_layer = [MicroService(env, name='pe0', average_work=0.030)]

    layers = [
        client_layer,
        frontend_layer,
        caching_layer,
        business_layer,
        persistence_layer,
    ]

    #
    # Horizontal wiring
    #

    # Connect layer n-1 to all micro-services in layer n
    for caller_layer, callee_layer in pairwise(layers):
        for caller_microservice in caller_layer:
            for callee_microservice in callee_layer:
                caller_microservice.connect_to(callee_microservice)

    #
    # Vertical wiring
    #
    virtual_machines = []
    for layer in layers:
        if layer == client_layer:
            # Clients come with their own infrastructure
            continue
        for microservice in layer:
            virtual_machine = VirtualMachine(env, num_cpus=8)
            virtual_machine.name = 'vm_' + str(microservice)
            microservice.run_on(virtual_machine)
            # TODO: Optionally add a VM to PM mapping algorithm.
            virtual_machine.run_on(physical_machines[0])
            virtual_machines.append(virtual_machine)

    #
    # Configure schedulers
    #
    for physical_machine in physical_machines:
        physical_machine.set_scheduler(method)
    for virtual_machine in virtual_machines:
        virtual_machine.set_scheduler(method)

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
    response_times = sum([client.response_times for client in client_layer], [])

    #
    # HACK: Sanity check
    #
    expected_cpu_time = sum([
        us.total_work for layer in layers if layer is not client_layer
        for us in layer])

    # Ensure VMs did not produce more CPU that requests could have consumed
    actual_vm_cpu_time = sum([vm.cpu_time for vm in virtual_machines])
    assert_almost_equal(actual_vm_cpu_time, expected_cpu_time,
                        'VM CPU time check failed')

    # Same for PMs
    actual_pm_cpu_time = sum([pm.cpu_time for pm in physical_machines])
    assert_almost_equal(actual_pm_cpu_time, expected_cpu_time,
                        'PM CPU time check failed')

    return [
        Result(
            arrival_rate=arrival_rate,
            method=method,
            response_time=response_time,
        )
        for response_time in response_times]


def main(output_filename='results.csv'):
    """
    Entry-point for simulator.
    Simulate the system for each method and output results.
    """

    arrival_rates = range(70, 80)
    methods = [
        'fifo',
        'ps',
        'tt',
        'tt+p',
    ]

    workers = multiprocessing.Pool() # pylint: disable=no-member
    futures = []
    for arrival_rate in arrival_rates:
        for method in methods:
            future = workers.apply_async(
                run_simulation,
                kwds=dict(arrival_rate=arrival_rate, method=method))
            futures.append(future)

    with open(output_filename, 'w') as output_file:
        fieldnames = Result._fields # pylint: disable=protected-access
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()

        for future in futures:
            results = future.get()
            print('completed: arrival_rate={0},method={1}'.format(
                results[0].arrival_rate, results[1].method), file=sys.stderr)
            for result in results:
                row = result._asdict() # pylint: disable=protected-access
                writer.writerow(row)

if __name__ == "__main__":
    main()
