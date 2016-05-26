#!/usr/bin/env python3
import collections
import csv
import random

Result = collections.namedtuple('Result', 'arrival_rate method response_time')

def run_simulation(arrival_rate, method):
    #create_clients()
    #create_micro_services()
    #create_infrastructure()
    #map_micro_services_to_infrastructure()

    #set_scheduler(method)

    #run()

    # TODO
    mean_response_time = 1.0
    response_times = [random.expovariate(1.0/mean_response_time*arrival_rate)
                      for _ in range(0, 1000)]
    return [
        Result(
            arrival_rate=arrival_rate,
            method=method,
            response_time=response_time,
        )
        for response_time in response_times]

def main(output_filename='results.csv'):
    """
    Entry-point for simulator.
    Simulate the system for each method and output results.
    """

    with open(output_filename, 'w') as output_file:
        writer = csv.DictWriter(output_file, fieldnames=Result._fields)
        writer.writeheader()
        for arrival_rate in range(10, 100, 10):
            for method in ['fifo', 'tail-tamer-without-preemption', 'tail-tamer-with-preemption']:
                results = run_simulation(
                    arrival_rate=arrival_rate,
                    method=method)
                for result in results:
                    writer.writerow(result._asdict())

if __name__ == "__main__":
    main()
