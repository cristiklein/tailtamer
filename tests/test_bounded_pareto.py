from .context import tailtamer

from math import pow
import random

bounded_pareto = tailtamer.bounded_pareto

def mean(x):
    if len(x) == 0:
        return float('nan')
    return sum(x)/len(x)

def variance(x):
    if len(x) == 0:
        return float('nan')

    return sum([_**2 for _ in x])/len(x) - mean(x)

def expected_mean(alpha, L, H):
    return (
            (pow(L, alpha) / (1 - pow(L/H, alpha))) *
            (alpha / (alpha - 1)) *
            (1 / pow(L, alpha - 1) - 1 / pow(H, alpha - 1))
           )

def expected_variance(alpha, L, H):
    return (
            (pow(L, alpha) / (1 - pow(L/H, alpha))) *
            (alpha / (alpha - 2)) *
            (1 / pow(L, alpha - 2) - 1 / pow(H, alpha - 2))
           )

def assert_approximately_equal(expected, actual, msg):
    if expected * 0.9 < actual < expected * 1.1:
        return
    raise AssertionError('{0}: expected {1}, actual {2}'.format(msg, expected,
        actual))

# Basic sanity check
def test_bounded_pareto():
    n = 10000

    alpha = 2.1
    L = 100
    H = 1000

    rng = random.Random()
    x = [ bounded_pareto(rng, alpha, L, H) for _ in range(0, n) ]

    # Extremely crude
    assert_approximately_equal(expected_mean(alpha, L, H), mean(x),
        "Mean should be around expected mean")
    assert_approximately_equal(expected_variance(alpha, L, H), variance(x),
        "Variance should be around expected variance")
    
