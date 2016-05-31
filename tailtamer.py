#!/usr/bin/env python3
"""
Main module of the simulator. Processes input to simulation, steers simulation and outputs results.
"""

import collections
from contextlib import contextmanager
import csv
from itertools import tee
import random

import simpy

Result = collections.namedtuple('Result', 'arrival_rate method response_time')
TraceItem = collections.namedtuple('TraceItem', 'who direction')

# LEFT HERE:
# We need to create some meta-concepts:
# - Tasks: something that has a certain about of work (duration) to perform.
# - Worker: something that produces tasks.
# - Executor: something that can execute (consume) tasks.
# - Scheduler: something that can decide what to run next and for how long.
#
# Questions:
# How should stuff be executed? Worker pushes tasks to executor? Executer pulls tasks from worker?
# How to handle the special case when the executor is idle?

def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)

def create_scheduler(method):
    pass

class PhysicalMachine(object):
    """
    Simulates a physical machine.
    """
    def __init__(self, env, num_cpus):
        # TODO
        pass

    def set_scheduler(self, scheduler):
        # TODO
        pass

class VirtualMachine(object):
    """
    Simulates a virtual machine.
    """
    def __init__(self, env, num_cpus, name=None):
        self._env = env
        self._cpus = simpy.PreemptiveResource(env, num_cpus)
        # TODO: str(id(self)) is not idempotent.
        self._name = 'vm_' + str(id(self))
        if name:
            self._name = name

    def run_on(self, executor):
        # TODO
        pass

    def set_scheduler(self, scheduler):
        # TODO
        pass

    def execute(self, request, work):
        with request.do_trace(self):
            with self._cpus.request(priority=request.start_time, preempt=True) as req:
                yield req
                try:
                    yield self._env.timeout(work)
                except simpy.Interrupt as interrupt:
                    by = interrupt.cause.by
                    usage = self._env.now - interrupt.cause.usage_since
                    print('%s got preempted by %s at %s after %s' %
                            (self._name, by, self._env.now, usage))

    def get_name(self):
        return self._name

    def set_name(self, name):
        self._name = name

    name = property(get_name, set_name)

    def __str__(self):
        return self._name

class Request(object):
    """
    Represents a request, travelling horizontally and vertically through the system.
    Only the client is allowed to create new requests.
    """
    def __init__(self, start_time):
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

    @contextmanager
    def do_trace(self, who):
        self._trace.append(TraceItem(who=who, direction='enter'))
        yield
        self._trace.append(TraceItem(who=who, direction='exit'))

    @property
    def trace(self):
        return self._trace

class OpenLoopClient(object):
    """
    Simulates open-loop clients, with a given arrival rate.
    """
    def __init__(self, env, arrival_rate):
        self._arrival_rate = arrival_rate
        self._env = env
        self._downstream_microservice = None
        self._requests = []
        self._random = random.Random()

        self._env.process(self.run())

    def connect_to(self, microservice):
        self._downstream_microservice = microservice

    def run(self):
        while True:
            self._env.process(self._on_arrival())
            waiting_time = self._random.expovariate(self._arrival_rate)
            yield self._env.timeout(waiting_time)

    def _on_arrival(self):
        request = Request(start_time=self._env.now)
        yield self._env.process(self._downstream_microservice.on_request(request))
        request.end_time = self._env.now
        self._requests.append(request)
        #for who, direction in request.trace:
        #    print(who, direction)
        #print()

    @property
    def response_times(self):
        return [r.end_time - r.start_time for r in self._requests]

class MicroService(object):
    """
    Simulates a micro-service, with a given average work and downcall structure.
    Currently, the execution model assumes one thread is created for each
    request.
    """
    def __init__(self, env, name, average_work):
        self._env = env
        self._name = name
        self._average_work = average_work
        self._executor = None
        self._downstream_microservices = []

    def run_on(self, executor):
        self._executor = executor

    def connect_to(self, microservice):
        self._downstream_microservices.append(microservice)

    def on_request(self, request):
        with request.do_trace(self):
            # TODO: Add randomness to demand. Variance might be a command-line parameter.
            demand = self._average_work
            demand_between_calls = demand / (len(self._downstream_microservices)+1)

            yield self._env.process(self._compute(request, demand_between_calls))
            for microservice in self._downstream_microservices:
                yield self._env.process(microservice.on_request(request))
                yield self._env.process(self._compute(request, demand_between_calls))

    def _compute(self, request, demand):
        yield self._env.process(self._executor.execute(request, demand))

    def __str__(self):
        return self._name

def run_simulation(arrival_rate, method, physical_machines=1):
    """
    Wire the simulation entities together, run one simulation and collect results.
    """

    #
    # Simulation environment
    #
    env = simpy.Environment()

    #
    # Infrastructure layer
    #
    physical_machines = [
        PhysicalMachine(env, num_cpus=4)
        for _ in range(physical_machines)
    ]

    #
    # Software layer
    #
    client_layer = [OpenLoopClient(env, arrival_rate=arrival_rate)]
    frontend_layer = [MicroService(env, name='fe0', average_work=0.001)]
    caching_layer = [MicroService(env, name='ca0', average_work=0.001)]
    business_layer = [MicroService(env, name='bu0', average_work=0.010)]
    persistence_layer = [MicroService(env, name='pe0', average_work=0.100)]

    layers = [client_layer, frontend_layer, caching_layer, business_layer, persistence_layer]

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
        physical_machine.set_scheduler(create_scheduler(method))
    for virtual_machine in virtual_machines:
        virtual_machine.set_scheduler(create_scheduler(method))

    #
    # Run simulation
    #
    env.run(until=100)

    #
    # Collect data
    #
    response_times = sum([client.response_times for client in client_layer], [])

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

    with open(output_filename, 'w') as output_file:
        writer = csv.DictWriter(output_file, fieldnames=Result._fields)
        writer.writeheader()
        for arrival_rate in range(10, 80, 10):
            for method in ['fifo', 'tail-tamer-without-preemption', 'tail-tamer-with-preemption']:
                results = run_simulation(
                    arrival_rate=arrival_rate,
                    method=method)
                for result in results:
                    writer.writerow(result._asdict())

if __name__ == "__main__":
    main()