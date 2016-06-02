from .context import tailtamer

import simpy
import sys

def generate_one_request(env, vm, requests, start_time=0, this_vm_start_time=0):
    request = tailtamer.Request(start_time)
    requests.append(request)

    def proc():
        yield env.timeout(this_vm_start_time)
        work = tailtamer.Work(env, 1)
        yield env.process(vm.execute(request, work))
        request.end_time = env.now
    
    env.process(proc())

def test_fifo():
    env = simpy.Environment()
    vm = tailtamer.VirtualMachine(env, num_cpus=1)

    requests = []

    generate_one_request(env, vm, requests)
    generate_one_request(env, vm, requests)

    env.run()

    assert requests[0].start_time==0
    assert requests[1].start_time==0

    assert abs(requests[0].end_time - 1.000) < 0.001
    assert abs(requests[1].end_time - 2.000) < 0.001

    assert abs(vm.cpu_time - 2) < 0.001

def test_ps():
    env = simpy.Environment()
    vm = tailtamer.VirtualMachine(env, num_cpus=1)
    vm.set_scheduler('ps')

    requests = []

    generate_one_request(env, vm, requests)
    generate_one_request(env, vm, requests)

    env.run()

    assert requests[0].start_time==0
    assert requests[1].start_time==0

    assert abs(requests[0].end_time - 1.995) < 0.001
    assert abs(requests[1].end_time - 2.000) < 0.001

    assert abs(vm.cpu_time - 2) < 0.001

def test_tail_tamer_without_preemption():
    env = simpy.Environment()
    vm = tailtamer.VirtualMachine(env, num_cpus=1)
    vm.set_scheduler('tt')

    requests = []

    generate_one_request(env, vm, requests, start_time=0.000, this_vm_start_time=0.102)
    generate_one_request(env, vm, requests, start_time=0.001, this_vm_start_time=0.101)
    generate_one_request(env, vm, requests, start_time=0.002, this_vm_start_time=0.100)

    env.run()

    assert abs(requests[0].end_time - 1.105) < 0.001, requests[0].end_time
    assert abs(requests[1].end_time - 2.105) < 0.001
    assert abs(requests[2].end_time - 3.100) < 0.001

    assert abs(vm.cpu_time - 3) < 0.001, vm.cpu_time

def test_tail_tamer_with_preemption():
    env = simpy.Environment()
    vm = tailtamer.VirtualMachine(env, num_cpus=1)
    vm.set_scheduler('tt+p')

    requests = []

    generate_one_request(env, vm, requests, start_time=0.000, this_vm_start_time=0.102)
    generate_one_request(env, vm, requests, start_time=0.001, this_vm_start_time=0.101)
    generate_one_request(env, vm, requests, start_time=0.002, this_vm_start_time=0.100)

    env.run()

    assert abs(requests[0].end_time - 1.102) < 0.001, requests[0].end_time
    assert abs(requests[1].end_time - 2.102) < 0.001
    assert abs(requests[2].end_time - 3.100) < 0.001

    assert vm.cpu_time == 3
