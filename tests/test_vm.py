from .context import tailtamer

import simpy
import sys

def generate_one_request(env, vm, requests, start_time=0, this_vm_start_time=0):
    request = tailtamer.Request(env.to_time(start_time))
    requests.append(request)

    def proc():
        yield env.timeout(env.to_time(this_vm_start_time))
        work = tailtamer.Work(env, env.to_time(1))
        yield env.process(vm.execute(request, work))
        request.end_time = env.now
    
    env.process(proc())

def test_fifo():
    env = tailtamer.NsSimPyEnvironment()
    vm = tailtamer.VirtualMachine(env, num_cpus=1)
    vm.set_scheduler('fifo')

    requests = []

    generate_one_request(env, vm, requests)
    generate_one_request(env, vm, requests)

    env.run()

    assert requests[0].start_time==0
    assert requests[1].start_time==0

    assert requests[0].end_time==env.to_time('1.000'), requests[0].end_time
    assert requests[1].end_time==env.to_time('2.000'), requests[1].end_time

    assert vm.cpu_time==2

def test_ps():
    env = tailtamer.NsSimPyEnvironment()
    vm = tailtamer.VirtualMachine(env, num_cpus=1)
    vm.set_scheduler('ps')

    requests = []

    generate_one_request(env, vm, requests)
    generate_one_request(env, vm, requests)

    env.run()

    assert requests[0].start_time==0
    assert requests[1].start_time==0

    assert requests[0].end_time==env.to_time('1.995')
    assert requests[1].end_time==env.to_time('2.000')

    assert vm.cpu_time==2

def test_tail_tamer_without_preemption():
    env = tailtamer.NsSimPyEnvironment()
    vm = tailtamer.VirtualMachine(env, num_cpus=1)
    vm.set_scheduler('tt')

    requests = []

    generate_one_request(env, vm, requests, start_time='0.000',
            this_vm_start_time='0.102')
    generate_one_request(env, vm, requests, start_time='0.001',
            this_vm_start_time='0.101')
    generate_one_request(env, vm, requests, start_time='0.002',
            this_vm_start_time='0.100')

    env.run()

    assert requests[0].end_time==env.to_time('1.105'), requests[0].end_time
    assert requests[1].end_time==env.to_time('2.105')
    assert requests[2].end_time==env.to_time('3.100')

    assert vm.cpu_time==3, vm.cpu_time

def test_tail_tamer_with_preemption():
    env = tailtamer.NsSimPyEnvironment()
    vm = tailtamer.VirtualMachine(env, num_cpus=1)
    vm.set_scheduler('tt+p')

    requests = []

    generate_one_request(env, vm, requests, start_time='0.000',
            this_vm_start_time='0.102')
    generate_one_request(env, vm, requests, start_time='0.001',
            this_vm_start_time='0.101')
    generate_one_request(env, vm, requests, start_time='0.002',
            this_vm_start_time='0.100')

    env.run()

    assert requests[0].end_time==env.to_time('1.102'), requests[0].end_time
    assert requests[1].end_time==env.to_time('2.101'), requests[1].end_time
    assert requests[2].end_time==env.to_time('3.100')

    assert vm.cpu_time==3

def test_tail_tamer_with_preemption_nested():
    env = tailtamer.NsSimPyEnvironment()
    vm = tailtamer.VirtualMachine(env, num_cpus=1)
    pm = tailtamer.PhysicalMachine(env, num_cpus=1)
    vm.run_on(pm)
    vm.set_scheduler('tt+p')
    pm.set_scheduler('tt+p')

    requests = []

    generate_one_request(env, vm, requests,
        start_time='0.000', this_vm_start_time='0.102')
    generate_one_request(env, vm, requests,
        start_time='0.001', this_vm_start_time='0.101')
    generate_one_request(env, vm, requests,
        start_time='0.002', this_vm_start_time='0.100')

    env.run()

    print([r.end_time for r in requests])

    assert requests[0].end_time==env.to_time('1.102'), requests[0].end_time
    assert requests[1].end_time==env.to_time('2.101'), requests[1].end_time
    assert requests[2].end_time==env.to_time('3.100'), requests[2].end_time

    assert vm.cpu_time==3
    assert pm.cpu_time==3, pm.cpu_time
