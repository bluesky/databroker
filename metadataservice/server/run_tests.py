#!/usr/bin/env python
import sys
import pytest

if __name__ == '__main__':
    args = ['-v']
    args.append('-rxs')
    args.extend(sys.argv[1:])
    sys.exit(pytest.main(args))
