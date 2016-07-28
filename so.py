#!/usr/bin/env python3
"""
Simulate the Stackoverflow architecture:
https://nickcraver.com/blog/2016/02/17/stack-overflow-the-architecture-2016-edition/
"""

import csv
import logging
import multiprocessing
import traceback
import sys

from tailtamer import \
    assert_equal, pretty_kwds, _trace_request
    
from tailtamer import MicroService, NsSimPyEnvironment, OpenLoopClient, PhysicalMachine, Result, VirtualMachine

class MicroServiceNg(MicroService):
    def connect_to(self, microservices, degree):
        assert int(degree) == degree or degree < 1
        assert type(microservices) == list

        self._downstream_microservices.append((microservices, degree))

    @_trace_request
    def on_request(self, request):
        demand = max(
            self._random.normalvariate(self._average_work, self._variance), 0)

        actual_calls = []
        for microservices, degree in self._downstream_microservices:
            microservice = self._random.choice(microservices)
            if degree >= 1:
                for _ in range(0, degree):
                    actual_calls.append(microservice)
            else:
                to_call = self._random.uniform(0, 1) < degree
                if to_call:
                    actual_calls.append(microservice)

        num_computations = len(actual_calls)+1
        demand_between_calls = self._env.to_time(demand / num_computations)

        yield self._env.process(
            self._compute(request, demand_between_calls))
        for microservice in actual_calls:
            yield self._env.process(microservice.on_request(request))
            yield self._env.process(
                self._compute(request, demand_between_calls))

def run_simulation(
        method,
        method_param=None,
        arrival_rate=2423,
        context_switch_overhead=0,
        num_physical_machines=1,
        num_physical_cpus=16,
        relative_variance=0.1,
        simulation_duration=10,
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
    clients = [
        OpenLoopClient(env, seed=seed,
                       arrival_rate=arrival_rate/4, until=simulation_duration),
        OpenLoopClient(env, seed=seed,
                       arrival_rate=arrival_rate/4, until=simulation_duration),
        OpenLoopClient(env, seed=seed,
                       arrival_rate=arrival_rate/4, until=simulation_duration),
        OpenLoopClient(env, seed=seed,
                       arrival_rate=arrival_rate/4, until=simulation_duration),
    ]


    haproxy = [
        MicroServiceNg(env, seed=seed, name='haproxy'+str(i),
            average_work=0.000001, relative_variance=relative_variance)
        for i in range(4)
    ]
    web = [
        MicroServiceNg(env, seed=seed, name='web'+str(i),
            average_work=0.0242, relative_variance=relative_variance)
        for i in range(4)
    ]
    redis = [
        MicroServiceNg(env, seed=seed, name='redis'+str(i),
            average_work=0.00000178, relative_variance=relative_variance)
        for i in range(2)
    ]
    search = [
        MicroServiceNg(env, seed=seed, name='search'+str(i),
            average_work=0.00541, relative_variance=relative_variance)
        for i in range(3)
    ]
    tag = [
        MicroServiceNg(env, seed=seed, name='tag'+str(i),
            average_work=0.0249, relative_variance=relative_variance)
        for i in range(3)
    ]
    db = [
        MicroServiceNg(env, seed=seed, name='db'+str(i),
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

    #
    # Vertical wiring
    #
    virtual_machines = []
    vm_id = 0
    for microservice in microservices:
        virtual_machine = VirtualMachine(env, num_cpus=16)
        virtual_machine.name = 'vm_' + str(microservice)
        microservice.run_on(virtual_machine)

        # round-robin VM to PM mapping
        physical_machine = physical_machines[vm_id % num_physical_machines]
        vm_id += 1
        virtual_machine.run_on(physical_machine)
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

    return [
        Result(
            response_time=response_time,
        )
        for response_time in response_times]

def main(name='num_physical_cpus', values=[19, 20, 21, 22], output_name=None,
        output_values=None, output_filename='results-so.csv'):
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
    ]

    workers = multiprocessing.Pool() # pylint: disable=no-member
    futures = []
    for method, method_param, use_tied_requests in method_param_tie_tuples:
        for value, output_value in zip(values, output_values):
            kwds = dict(method=method, method_param=method_param)
            kwds[name] = value

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
        fieldnames = list(future.kwds.keys())
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

if __name__ == "__main__":
    main()
