import itertools

def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = itertools.tee(iterable) # pylint: disable=invalid-name
    next(b, None)
    return zip(a, b)

def pretty_kwds(kwds, sep=' '):
    "pretty_kwds(a=1, b=2) -> 'a=1 b=2'"
    return sep.join(sorted([str(k)+'='+str(v) for k, v in kwds.items()]))

def bounded_pareto(rng, alpha, L, H):
    # pylint: disable=invalid-name
    U = rng.random()
    Ha = H**alpha
    La = L**alpha
    return (-(U*Ha - U*La - Ha)/(Ha * La)) ** (-1.0/alpha)

def assert_equal(actual, expected, message):
    """
    Asserts that two objects are equal.
    """
    if actual != expected:
        raise AssertionError('{0}: actual {1}, expected {2}'.format(
            message, actual, expected))

