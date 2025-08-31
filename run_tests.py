import pytest
import sys

if __name__ == "__main__":
    # Run pytest programmatically
    args = ["-q", "tests"]
    exit_code = pytest.main(args)
    sys.exit(exit_code)
