from tailtamer import \
        Request, Work, VirtualMachine, PhysicalMachine, NsSimPyEnvironment, Thread

import simpy
import sys

def generate_one_request(env, vm, requests, start_time=0, this_vm_start_time=0,
        work=1):
    request = Request(env.to_time(start_time))
    requests.append(request)

    def proc():
        yield env.timeout(env.to_time(this_vm_start_time))
        w = Work(env, env.to_time(work), request)
        sched_entity = Thread()
        yield env.process(vm.execute(sched_entity, request, w))
        request.end_time = env.now
    
    env.process(proc())

def assert_equal(actual, expected):
    if actual!=expected:
        raise AssertionError("Expected: {0}, actual {1}".format(expected,
            actual))

def test_fifo():
    env = NsSimPyEnvironment()
    vm = VirtualMachine(env, num_cpus=1)
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
    env = NsSimPyEnvironment()
    vm = VirtualMachine(env, num_cpus=1)
    vm.set_scheduler('ps')

    requests = []

    generate_one_request(env, vm, requests)
    generate_one_request(env, vm, requests)

    generate_one_request(env, vm, requests, this_vm_start_time=3)
    generate_one_request(env, vm, requests, this_vm_start_time=3.5)
    generate_one_request(env, vm, requests, this_vm_start_time=4)

    env.run()

    assert_equal(requests[0].start_time, 0)
    assert_equal(requests[1].start_time, 0)

    assert_equal(requests[0].end_time, env.to_time('2.000'))
    assert_equal(requests[1].end_time, env.to_time('2.000'))

    assert_equal(requests[2].end_time, env.to_time('4.75'))
    assert_equal(requests[3].end_time, env.to_time('5.75'))
    assert_equal(requests[4].end_time, env.to_time('6'))

    assert_equal(vm.cpu_time, 5)

def test_tail_tamer_without_preemption():
    env = NsSimPyEnvironment()
    vm = VirtualMachine(env, num_cpus=1)
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
    env = NsSimPyEnvironment()
    vm = VirtualMachine(env, num_cpus=1)
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
    env = NsSimPyEnvironment()
    vm = VirtualMachine(env, num_cpus=1)
    pm = PhysicalMachine(env, num_cpus=1)
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

def test_ttlas():
    env = NsSimPyEnvironment()
    vm = VirtualMachine(env, num_cpus=1)
    vm.set_scheduler('ttlas')

    requests = []

    generate_one_request(env, vm, requests, this_vm_start_time=0, work=3)
    generate_one_request(env, vm, requests, this_vm_start_time=1, work=2)
    generate_one_request(env, vm, requests, this_vm_start_time=2, work=1)

    env.run()

    assert requests[0].end_time==6, requests[0].end_time
    assert requests[1].end_time==5, requests[1].end_time
    assert requests[2].end_time==3, requests[2].end_time

    assert vm.cpu_time==6, vm.cpu_time
