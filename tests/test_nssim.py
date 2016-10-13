from .context import tailtamer

from nose.tools import raises

import simpy
import sys

def some_process(env, start_at):
    yield env.timeout(start_at)

@raises(TypeError)
def test_ns_fail():
    """
    Ensure it is not possible to inject floats into simulation time.
    """
    env = tailtamer.NsSimPyEnvironment()
    env.process(some_process(env, 10.1))
    env.run()

@raises(TypeError)
def test_ns_fail2():
    """
    Ensure it is not possible to inject floats into simulation time.
    """
    env = tailtamer.NsSimPyEnvironment()
    env.process(some_process(env, 4e-29))
    env.run()

def test_ns_ok1():
    """
    Integer timeout should be okey
    """
    env = tailtamer.NsSimPyEnvironment()
    env.process(some_process(env, env.to_time(10)))
    env.run()
    assert env.now == 10

def test_ns_ok2():
    """
    Integer timeout should be okey
    """
    env = tailtamer.NsSimPyEnvironment()
    env.process(some_process(env, env.to_time('10.1')))
    env.run()
    env.process(some_process(env, env.to_time('0.9')))
    env.run()
    assert env.now == 11
