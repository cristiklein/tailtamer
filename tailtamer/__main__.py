"""
Main module of the simulator. Processes input to simulation, steers simulation
and outputs results.
"""
import csv
import logging
import multiprocessing
import time

from .application import *
from .base import Result
from .simulation import run_simulation
from .util import pretty_kwds

def explore_param(output_filename, name, values, output_name=None,
                  output_values=None, **simulation_kwds):
    """
    Runs several simulations for all scheduling methods, varying the given
    parameter along the given values, writing the results to the given output
    filename.
    """
    if output_name is None:
        output_name = name
    if output_values is None:
        output_values = values

    logger = logging.getLogger('tailtamer')
    logger.info("Exploring %s for %s", output_name,
                ' '.join([str(value) for value in output_values]))

    method_param_tie_tuples = [
        # Disabled for now: always worse than BVT and less convincing
        #('cfs' , None   , False), # pylint: disable=bad-whitespace
        ('bvt' , None   , False), # pylint: disable=bad-whitespace
        ('bvt' , None   , True ), # pylint: disable=bad-whitespace
        ('fifo', None   , False), # pylint: disable=bad-whitespace
        ('ps'  , None   , False), # pylint: disable=bad-whitespace
        ('tt'  , '0.005', False), # pylint: disable=bad-whitespace
        ('tt+p', None   , False), # pylint: disable=bad-whitespace
        # Disabled for now: gives really bad performance
        #('ttlas', '0.001', False), # pylint: disable=bad-whitespace
    ]

    workers = multiprocessing.Pool() # pylint: disable=no-member
    futures = []
    for method, method_param, use_tied_requests in method_param_tie_tuples:
        for value, output_value in zip(values, output_values):
            kwds = dict(method=method, method_param=method_param)
            kwds.update(simulation_kwds)
            kwds[name] = value
            if use_tied_requests:
                if name is 'layers_config':
                    kwds['layers_config'] = with_tied_requests(value)
                else:
                    kwds['layers_config'] = \
                        with_tied_requests(DEFAULT_LAYERS_CONFIG)

            future = workers.apply_async(
                run_simulation,
                kwds=kwds)
            future.kwds = dict(kwds)
            future.kwds['method'] = \
                method + ('_' + str(method_param) if method_param else '') + \
                ('+tie' if use_tied_requests else '')
            if 'layers_config' in future.kwds:
                del future.kwds['layers_config']
            future.kwds[output_name] = output_value
            futures.append(future)

    with open(output_filename, 'w') as output_file:
        fieldnames = sorted(list(future.kwds.keys()))
        fieldnames += Result._fields # pylint: disable=protected-access
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()

        for future in futures:
            results = future.get()
            logger.info('Simulation completed: %s', pretty_kwds(future.kwds))
            for result in results:
                row = result._asdict() # pylint: disable=protected-access
                row.update(future.kwds)
                writer.writerow(row)

def main():
    """
    Entry-point for simulator.
    Simulate the system for each method and output results.
    """

    logger = logging.getLogger('tailtamer')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    channel = logging.StreamHandler()
    channel.setFormatter(formatter)
    logger.addHandler(channel)

    started_at = time.time()
    logger.info('Starting simulations')

    loads = [0.8, 0.825, 0.85, 0.875, 0.9, 0.925, 0.95, 0.975]
    explore_param(
        'results-ar.csv', 'arrival_rate',
        [load*160.0 for load in loads],
        output_name='load',
        output_values=loads)

    # Variance
    explore_param(
        'results-var.csv', 'layers_config',
        [
            with_relative_variance(DEFAULT_LAYERS_CONFIG, 0.00),
            with_relative_variance(DEFAULT_LAYERS_CONFIG, 0.05),
            with_relative_variance(DEFAULT_LAYERS_CONFIG, 0.10),
            with_relative_variance(DEFAULT_LAYERS_CONFIG, 0.20),
        ],
        output_name='relative_variance',
        output_values=[0.00, 0.05, 0.10, 0.20])

    # Degree
    explore_param(
        'results-deg.csv', 'layers_config',
        [
            with_last_degree(DEFAULT_LAYERS_CONFIG, 1),
            with_last_degree(DEFAULT_LAYERS_CONFIG, 2),
            with_last_degree(DEFAULT_LAYERS_CONFIG, 5),
            with_last_degree(DEFAULT_LAYERS_CONFIG, 10),
        ],
        output_name='degree',
        output_values=[1, 2, 5, 10])

    # Multiplicity
    explore_param(
        'results-mul.csv', 'layers_config',
        [
            with_last_multiplicity(DEFAULT_LAYERS_CONFIG, 1),
            with_last_multiplicity(DEFAULT_LAYERS_CONFIG, 2),
            with_last_multiplicity(DEFAULT_LAYERS_CONFIG, 5),
            with_last_multiplicity(DEFAULT_LAYERS_CONFIG, 10),
        ],
        output_name='multiplicity',
        output_values=[1, 2, 5, 10])

    # Context-switch overhead
    explore_param(
        'results-ctx.csv', 'context_switch_overhead',
        ['0', '0.000001', '0.000010', '0.000100'])

    # Stackoverflow Architecture
    explore_param(
        'results-so.csv',
        'num_physical_cpus', [21, 22, 23, 24],
        arrival_rate=2423,
        context_switch_overhead='0.000001',
        software_layer_generator=so_microservices,
        simulation_duration=10)

    ended_at = time.time()
    logger.info('Simulations completed in %f seconds', ended_at-started_at)

if __name__ == "__main__":
    main()
