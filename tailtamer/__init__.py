"""
Package to simulate tail response time of complex applications in virtualized or
non-virtualized environments. Implements concepts and wires them together.
"""

from tailtamer.request import Request, Work
from tailtamer.machine import VirtualMachine, PhysicalMachine
from tailtamer.simulation import NsSimPyEnvironment
from tailtamer.thread import Thread
