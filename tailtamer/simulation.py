import decimal
import sys
import traceback

import simpy

from .application import DEFAULT_LAYERS_CONFIG, layered_microservices
from .base import Result
from .machine import PhysicalMachine
from .util import assert_equal

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
    us_id = 0
    for microservice in microservices:
        # Round-robin micro-service to PM mapping
        physical_machine = physical_machines[us_id % num_physical_machines]
        microservice.run_on(physical_machine)
        us_id += 1

    #
    # Configure schedulers
    #
    for physical_machine in physical_machines:
        physical_machine.set_scheduler(method, method_param)

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
