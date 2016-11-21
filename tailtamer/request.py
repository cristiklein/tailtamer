"""
Contains concepts related to requests, tracing them and the work they generate.
"""

import collections
import simpy

from .base import NamedObject

TraceItem = collections.namedtuple('TraceItem', 'when who direction')

def trace_request(instance_method):
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
        """
        Wraps a SimPy-style generator-based process, marking entry and exit of a
        request.
        """
        request.do_trace(who=instance, when=instance._env.now,
                         direction='enter')
        yield from instance_method(instance, request, *v, **k)
        request.do_trace(who=instance, when=instance._env.now,
                         direction='exit')
    return wrapper

class Cancelled(Exception):
    """
    Represents the fact that a request was cancelled, as requested by the user.
    """
    pass

class RequestTiePair(object):
    """
    Represents the information necessary to produce request ties. Tying requests
    is a technique popularised by Google (Tail at Scale) to reduce tail response
    time. It consists in sending two tied requests: When one starts, the other
    one is cancelled. To avoid the two requests cancelling each other out,
    symmetry is broken by marking one request as high-priority, while the other
    one as low-priority. Once started, the high-priority request can no longer
    be cancelled, whereas the low-priority request can be cancelled after
    startal.
    """
    def __init__(self, env):
        self._start_event_high_prio = simpy.events.Event(env)
        self._start_event_low_prio = simpy.events.Event(env)

    @property
    def high_prio(self):
        """
        Returns a tuple necessary for the high-priority request:
        cancel_after_start, start_event, cancel_event. The work is supposed to
        trigger `start_event` on startal and cancel itself when `cancel_event`
        is triggered.
        """
        return False, self._start_event_high_prio, self._start_event_low_prio

    @property
    def low_prio(self):
        """
        Returns a tuple necessary for the high-priority request:
        cancel_after_start, start_event, cancel_event
        """
        return True, self._start_event_low_prio, self._start_event_high_prio

class Work(object):
    """
    Simulates work that has to be performed. Work must only be created by
    microservices and consumed by lowest-level executors.
    """
    def __init__(self, env, work, request, tie=None):
        assert work >= 0

        self._env = env
        self._initial = work
        self._remaining = work
        self._process = None
        self._cancelled = False
        self._request = request
        self._tie = tie

        if self._tie:
            cancel_event = self._tie[2]
            cancel_event.callbacks.append(self.cancel)

    def consume(self, max_work_to_consume, inverse_rate=1):
        """
        Consumes work, i.e., sleeps for a given maximum amount of time.  Work is
        currently equal to time, but may in future be modulated, e.g., due to
        frequency scaling. The method returns as soon as either all work is
        consumed or the maximum amount, given as parameter, is reached.
        """
        assert max_work_to_consume > 0
        assert self._process is None

        cancel_after_start, start_event, cancel_event = \
            self._tie if self._tie else (False, None, None)

        if self._cancelled:
            # cancelled before startal
            raise Cancelled()

        self._process = self._env.active_process
        work_to_consume = min(self._remaining, max_work_to_consume)
        assert work_to_consume > 0
        try:
            started_at = self._env.now
            if not cancel_after_start and cancel_event and \
                    cancel_event.callbacks:
                cancel_event.callbacks.remove(self.cancel)
            if start_event and not start_event.triggered:
                start_event.succeed()
            yield self._env.timeout(work_to_consume*inverse_rate)
        except simpy.Interrupt as interrupt:
            if interrupt.cause == 'cancelled':
                assert self._cancelled
                raise Cancelled() from interrupt
            raise
        finally:
            ended_at = self._env.now
            self._remaining -= (ended_at-started_at)/inverse_rate
            self._process = None
            if cancel_event and cancel_event.callbacks:
                cancel_event.callbacks.remove(self.cancel)

    def cancel(self, _=None):
        """
        Cancel all outstanding work, if any is left.
        """
        if self.consumed or self._cancelled:
            return

        self._cancelled = True
        if self._process:
            self._process.interrupt(cause='cancelled')

    @property
    def cancelled(self):
        """
        True if the request was cancelled before completion.
        """
        return self._cancelled

    @property
    def amount_consumed(self):
        """
        Returns the amount of work consumed so far. We try not to return the
        amount of work remaining, to hide this information from the scheduler.
        """
        return self._initial-self._remaining

    @property
    def consumed(self):
        """
        Returns True if this work item was fully consumed, False otherwise.
        """
        return self._remaining == 0

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
        self._attained_time = 0

    @property
    def start_time(self):
        "The time the request entered the cloud application."
        return self._start_time

    def _get_end_time(self):
        "Get the time the request exited the cloud application."
        return self._end_time

    def _set_end_time(self, new_value):
        "Set the time the request exited the cloud application."
        assert self._end_time is None
        self._end_time = new_value

    end_time = property(_get_end_time, _set_end_time)

    def do_trace(self, when, who, direction):
        """
        Record a particular boundary event in the live of the request, such as
        entering or exiting a microservice, a VCPU or a CPU.
        """
        #print('{0:.6f} {1!s:<6} {2} {3}'.format(when, who, self, direction))
        self._trace.append(TraceItem(when=when, who=who, direction=direction))

    @property
    def trace(self):
        "Return the trace of this request."
        return self._trace

    @property
    def attained_time(self):
        "Return the amount of time this request has been processed so far."
        return self._attained_time

    def add_attained_time(self, time):
        "Adds attained time as required for the Least-Attained-Time scheduler."
        self._attained_time += time

