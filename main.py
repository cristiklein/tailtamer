#!/usr/bin/env python3
"""
Main module of the simulator. Processes input to simulation, steers simulation and outputs results.
"""

import collections
import csv
from itertools import tee
import random

Result = collections.namedtuple('Result', 'arrival_rate method response_time')

def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)

def create_scheduler(method):
    pass

class Environment(object):
    """
    Represent the simulation environment.

    TODO, likely with SimPy
    """
    def __init__(self):
        pass

    def run(self):
        """
        Runs the simulation, either to completion or up to a given time.
        """
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
    def __init__(self, env, num_cpus):
        # TODO
        pass

    def run_on(self, executor):
        # TODO
        pass

    def set_scheduler(self, scheduler):
        # TODO
        pass

class OpenLoopClient(object):
    """
    Simulates open-loop clients, with a given arrival rate.
    """
    def __init__(self, env, arrival_rate):
        # TODO
        self.arrival_rate = arrival_rate
        self.env = env

    def connect_to(self, caller):
        # TODO
        pass

    @property
    def response_times(self):
        mean_response_time = 1.0
        return [random.expovariate(1.0/mean_response_time*self.arrival_rate)
                for _ in range(0, 1000)]

class MicroService(object):
    """
    Simulates a micro-service, with a given average work and downcall structure.
    """
    def __init__(self, env, average_work, thread_pool_size=100):
        # TODO
        pass

    def run_on(self, executor):
        # TODO
        pass

    def connect_to(self, caller):
        # TODO
        pass

def run_simulation(arrival_rate, method, physical_machines=1):
    """
    Wire the simulation entities together, run one simulation and collect results.
    """

    #
    # Simulation environment
    #
    env = Environment()

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
    frontend_layer = [MicroService(env, average_work=0.001)]
    caching_layer = [MicroService(env, average_work=0.001)]
    business_layer = [MicroService(env, average_work=0.010)]
    persistence_layer = [MicroService(env, average_work=0.100)]

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
            virtual_machine = VirtualMachine(env, num_cpus=2)
            microservice.run_on(virtual_machine)
            # TODO: Mapping algorithm
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
    env.run()

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
        for arrival_rate in range(10, 100, 10):
            for method in ['fifo', 'tail-tamer-without-preemption', 'tail-tamer-with-preemption']:
                results = run_simulation(
                    arrival_rate=arrival_rate,
                    method=method)
                for result in results:
                    writer.writerow(result._asdict())

if __name__ == "__main__":
    main()
