#!/usr/bin/env python3
import collections
import csv
import random
import sys

Result = collections.namedtuple('Result', 'method response_times')

def run_simulation(method):
    # TODO
    mean_response_time = 1.0
    response_times = [random.expovariate(1.0/mean_response_time) for _ in range(0, 1000)]
    return Result(
        method=method,
        response_times=response_times,
    )

def main(outputFilename='results.csv'):
    """
    Entry-point for simulator.
    Simulate the system for each method and output results.
    """

    with open(outputFilename, 'w') as outputFile:
        writer = csv.DictWriter(outputFile, fieldnames=['method', 'response_time'])
        writer.writeheader()
        for method in ['fifo', 'tail-tamer-without-preemption', 'tail-tamer-with-preemption']:
            result = run_simulation(
                method=method)
            for response_time in result.response_times:
                writer.writerow({'method': method, 'response_time': response_time})

if __name__ == "__main__":
    main()
