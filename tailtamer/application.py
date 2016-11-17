import collections

from .client import OpenLoopClient
from .microservice import MicroService
from .util import pairwise

__all__ = [
    "DEFAULT_LAYERS_CONFIG",
    "with_relative_variance", "with_last_degree", "with_last_multiplicity",
    "with_tied_requests",
    "layered_microservices", "so_microservices"]

Layer = collections.namedtuple(
    'Layer', 'average_work relative_variance degree multiplicity '+
    'use_tied_requests')

DEFAULT_LAYERS_CONFIG = (
    Layer(average_work=0.001, relative_variance=0, degree=1, multiplicity=1,
          use_tied_requests=False),
    Layer(average_work=0.001, relative_variance=0, degree=1, multiplicity=1,
          use_tied_requests=False),
    Layer(average_work=0.010, relative_variance=0, degree=1, multiplicity=1,
          use_tied_requests=False),
    Layer(average_work=0.088, relative_variance=0, degree=1, multiplicity=1,
          use_tied_requests=False),
)

def with_relative_variance(template_layers_config, relative_variance):
    layers_config = [
        layer_config._replace(relative_variance=relative_variance) for
        layer_config in template_layers_config]
    return layers_config

def with_last_degree(template_layers_config, degree):
    assert len(template_layers_config) >= 2
    layers_config = [
        layer_config for
        layer_config in template_layers_config]
    layers_config[-1] = \
        layers_config[-1]._replace(average_work=layers_config[-1].average_work/degree)
    layers_config[-2] = \
        layers_config[-2]._replace(degree=degree)
    return layers_config

def with_last_multiplicity(template_layers_config, multiplicity):
    layers_config = [
        layer_config for
        layer_config in template_layers_config]
    layers_config[-1] = \
        layers_config[-1]._replace(
            average_work=layers_config[-1].average_work/multiplicity,
            multiplicity=multiplicity)
    return layers_config

def with_tied_requests(template_layers_config):
    layers_config = [
        layer_config for
        layer_config in template_layers_config]
    layers_config[-2] = \
        layers_config[-2]._replace(
            use_tied_requests=True)
    return layers_config

def layered_microservices(env, seed, simulation_duration, arrival_rate,
                          layers_config, **_):
    """
    Produces a layered micro-service architecture and a single client.
    """

    clients = [
        OpenLoopClient(env, seed=seed,
                       arrival_rate=arrival_rate, until=simulation_duration),
    ]

    microservices = []
    layers = []
    for c in layers_config: # pylint: disable=invalid-name
        layer = []
        for _ in range(c.multiplicity):
            microservice = MicroService(
                env, seed=seed,
                name='l{0}us{1}'.format(len(layers), len(layer)),
                average_work=c.average_work,
                variance=c.average_work*c.relative_variance,
                degree=c.degree,
                use_tied_requests=c.use_tied_requests,
            )
            microservices.append(microservice)
            layer.append(microservice)
        layers.append(layer)

    #
    # Horizontal wiring
    #

    # Connect layer n-1 to all micro-services in layer n
    for caller_layer, callee_layer in pairwise([clients] + layers):
        for caller_microservice in caller_layer:
            for callee_microservice in callee_layer:
                caller_microservice.connect_to(callee_microservice)

    return clients, microservices

def so_microservices(env, seed, simulation_duration, arrival_rate,
                     relative_variance=0.1, **_):
    """
    Produces a micro-service architecture similar to StackOverflow:
    https://nickcraver.com/blog/2016/02/17/stack-overflow-the-architecture-2016-edition/
    """

    clients = [
        OpenLoopClient(env, seed=seed,
                       arrival_rate=arrival_rate/4, until=simulation_duration),
        OpenLoopClient(env, seed=seed+1,
                       arrival_rate=arrival_rate/4, until=simulation_duration),
        OpenLoopClient(env, seed=seed+2,
                       arrival_rate=arrival_rate/4, until=simulation_duration),
        OpenLoopClient(env, seed=seed+3,
                       arrival_rate=arrival_rate/4, until=simulation_duration),
    ]


    haproxy = [
        MicroService(env, seed=seed, name='haproxy'+str(i),
                     average_work=0.000001, relative_variance=relative_variance)
        for i in range(4)
    ]
    web = [
        MicroService(env, seed=seed, name='web'+str(i),
                     average_work=0.0242, relative_variance=relative_variance)
        for i in range(4)
    ]
    redis = [
        MicroService(env, seed=seed, name='redis'+str(i),
                     average_work=0.00000178, relative_variance=relative_variance)
        for i in range(2)
    ]
    search = [
        MicroService(env, seed=seed, name='search'+str(i),
                     average_work=0.00541, relative_variance=relative_variance)
        for i in range(3)
    ]
    tag = [
        MicroService(env, seed=seed, name='tag'+str(i),
                     average_work=0.0249, relative_variance=relative_variance)
        for i in range(3)
    ]
    dba = [
        MicroService(env, seed=seed, name='db'+str(i),
                     average_work=0.0012, relative_variance=relative_variance)
        for i in range(4)
    ]

    microservices = haproxy + web + redis + search + tag + dba

    #
    # Horizontal wiring
    #
    for i in range(4):
        clients[i].connect_to(haproxy[i])

    for _us in haproxy:
        _us.connect_to(web, 0.31)

    for _us in web:
        _us.connect_to(redis, 28)
        _us.connect_to(tag, 0.017)
        _us.connect_to(search, 0.07)
        _us.connect_to(dba, 2)

    for _us in tag:
        _us.connect_to(dba, 1)

    return clients, microservices
