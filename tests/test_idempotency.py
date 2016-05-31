from .context import tailtamer

import difflib

# Ensure simulator is idempotent
def test_idempotency():
    tailtamer.main()
    with open('results.csv') as f:
        results1 = f.readlines()

    tailtamer.main()
    with open('results.csv') as f:
        results2 = f.readlines()

    diff = difflib.ndiff(results1, results2)

    assert results1 == results2, "Simulator should be idempotent: " + ''.join(diff)
