from .context import tailtamer

import simpy
import sys

# Basic sanity check
def test_time_slicing():
    env = simpy.Environment()
    vm = tailtamer.VirtualMachine(env, num_cpus=1)

    requests = []

    def generate_one_request():
        request = tailtamer.Request(env.now)
        yield env.process(vm.execute(request, 1))
        request.end_time = env.now
        requests.append(request)

    env.process(generate_one_request())
    env.process(generate_one_request())

    env.run()

    assert requests[0].start_time==0
    assert requests[1].start_time==0

    assert abs(requests[0].end_time - 1.995) < 0.001
    assert abs(requests[1].end_time - 2.000) < 0.001
