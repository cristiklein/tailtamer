import decimal
import simpy

from .base import NamedObject
from .request import Cancelled
from .thread import Thread

class VirtualMachine(NamedObject):
    """
    Simulates a virtual machine.
    """
    ALLOWED_SCHEDULERS = [
        'bvt',
        'cfs',
        'fifo',
        'ps',
        'tt',
        'tt+p',
        'ttlas',
    ]

    def __init__(self, env, num_cpus, prefix='vm', name=None,
                 context_switch_overhead=0):
        super().__init__(prefix=prefix, name=name)

        self._env = env
        self._cpus = simpy.PreemptiveResource(env, num_cpus)
        self._executor = None
        self.set_scheduler('ps', None)

        self._context_switch_overhead = \
            self._env.to_time(context_switch_overhead)
        ## stores last work performed on each CPU, to decide whether context
        # switching overhead should be added or not.
        self._cpu_cache = [None] * num_cpus
        ## keep track of which CPUs are busy
        self._cpu_is_idle = [True] * num_cpus
        ## conversion from VCPU to OS thread, à la type 2 hypervisor
        self._cpu_thread = [Thread(name=str(self)+'_cpu'+str(i)) for i in range(num_cpus)]

        self._cpu_time = 0

        self._num_active_cpus = 0

        self._runnable_sched_entities = set()
        self._min_vruntime = 0

        self._ps_processes = set()

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
            if scheduler in ['tt', 'ttlas']:
                timeslice = '0.005'
            else:
                timeslice = 'inf'
        self._scheduler = (scheduler, self._env.to_time(timeslice))

    #@_trace_request
    def execute(self, sched_entity, request, work, max_work_to_consume='inf'):
        """
        Consume the given work up to the given limit. When taking scheduling
        decisions, take into account that the work item is related to the
        given request.
        """
        # TODO: This method is waaay too large. However, splitting it up is
        # non-trivial if performance is to be maintained.

        max_work_to_consume = self._env.to_time(max_work_to_consume)
        executor = self._executor
        scheduler, timeslice = self._scheduler
        cpus = self._cpus
        cpu_request = self._cpus.request
        num_cpus = self._cpus.capacity

        if scheduler == 'ps':
            if executor is None:
                yield from self._execute_ps(request, work, max_work_to_consume)
                return
            else:
                raise NotImplementedError('ps with executor not implemented')

        sched_latency = self._env.to_time('0.024')
        sched_min_granularity = self._env.to_time('0.003')

        # A smaller number means higher priority.
        if scheduler in ['bvt', 'cfs']:
            preempt = False
            priority = 0
            # http://lxr.free-electrons.com/source/kernel/sched/fair.c#L457
            if self._runnable_sched_entities:
                min_vruntime = min([se.vruntime for se in self._runnable_sched_entities])
                self._min_vruntime = max(self._min_vruntime, min_vruntime)
            # http://lxr.free-electrons.com/source/kernel/sched/fair.c#L3262
            if scheduler == 'cfs':
                vruntime = self._min_vruntime - sched_latency/2
            # https://gist.github.com/leverich/5913713
            if scheduler == 'bvt':
                vruntime = self._min_vruntime + self._env.to_time('0.000001')
            sched_entity.vruntime = max(sched_entity.vruntime, vruntime)
        elif scheduler == 'fifo':
            preempt = False
            priority = self._env.now
        elif scheduler == 'tt':
            preempt = False
            priority = request.start_time
        elif scheduler == 'tt+p':
            preempt = True
            priority = request.start_time
        elif scheduler == 'ttlas':
            preempt = False
            priority = request.attained_time
        else:
            raise NotImplementedError() # should never get here

        self._runnable_sched_entities.add(sched_entity)
        try:
            while max_work_to_consume > 0 and not work.consumed:
                if scheduler == 'ttlas':
                    priority = request.attained_time
                if scheduler in ['bvt', 'cfs']:
                    priority = sched_entity.vruntime

                # For priority "A smaller number means higher priority."
                with cpu_request(priority=priority, preempt=preempt) as req:
                    try:
                        amount_consumed_before = work.amount_consumed
                        cpu_id = None

                        yield req

                        if scheduler in ['bvt', 'cfs']:
                            timeslice = self._env.to_time(
                                max(sched_latency/(len(cpus.users)+len(cpus.queue)),
                                    sched_min_granularity))
                        work_to_consume = min(timeslice, max_work_to_consume)

                        self._num_active_cpus += 1

                        assert self._num_active_cpus <= num_cpus, \
                            "Weird! Attempt to execute more requests "+\
                            "concurrently than available CPUs. There "+\
                            "is a bug in the simulator."

                        # This code is a bit cryptic to improve performance. In
                        # essence, we simulate CPU caches and track what work
                        # was execute on each CPU. If we find the CPU on which the
                        # current work was last executed, we consider the cache
                        # to be hot and no overhead is paid. Otherwise, we
                        # search for an idle CPU, consider the cache cold and
                        # pay the overhead.
                        cache_is_hot = False

                        some_idle_cpu_id = None
                        for cpu_id in range(len(self._cpu_is_idle)):
                            if self._cpu_cache[cpu_id] == work:
                                cache_is_hot = True
                                break
                            if self._cpu_is_idle[cpu_id]:
                                some_idle_cpu_id = cpu_id

                        if not cache_is_hot:
                            cpu_id = some_idle_cpu_id
                            self._cpu_cache[cpu_id] = work
                        self._cpu_is_idle[cpu_id] = False

                        if executor is None:
                            if not cache_is_hot:
                                yield self._env.timeout(self._context_switch_overhead)
                            yield from work.consume(work_to_consume)
                        else:
                            yield from executor.execute(self._cpu_thread[cpu_id], request, work,
                                                        work_to_consume)
                    except simpy.Interrupt as interrupt:
                        if interrupt.cause.resource == cpus:
                            pass
                        else:
                            raise
                    except Cancelled:
                        raise # propagate cancellation up
                    finally:
                        work_consumed = work.amount_consumed-amount_consumed_before
                        self._cpu_time += work_consumed
                        max_work_to_consume -= work_consumed
                        self._num_active_cpus -= 1
                        sched_entity.vruntime += work_consumed
                        if executor is None:
                            request.add_attained_time(work_consumed)
                        if cpu_id is not None:
                            self._cpu_is_idle[cpu_id] = True
        finally:
            self._runnable_sched_entities.remove(sched_entity)

    def _execute_ps(self, request, work, max_work_to_consume='inf'):
        self._ps_processes.add(self._env.active_process)
        self._ps_adjust_rate()

        num_cpus = self._cpus.capacity
        while max_work_to_consume > 0 and not work.consumed:
            try:
                amount_consumed_before = work.amount_consumed
                if len(self._ps_processes) <= num_cpus:
                    inverse_rate = 1
                else:
                    inverse_rate = decimal.Decimal(len(self._ps_processes)) / num_cpus
                yield from work.consume(max_work_to_consume,
                                        inverse_rate=inverse_rate)
            except simpy.Interrupt:
                pass
            except Cancelled:
                raise # propagate cancellation up
            finally:
                work_consumed = work.amount_consumed-amount_consumed_before
                self._cpu_time += work_consumed
                max_work_to_consume -= work_consumed
                request.add_attained_time(work_consumed)

        self._ps_processes.remove(self._env.active_process)
        self._ps_adjust_rate()

    def _ps_adjust_rate(self):
        self._ps_processes.remove(self._env.active_process)
        for process in self._ps_processes:
            if process != self._env.active_process:
                process.interrupt()

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


