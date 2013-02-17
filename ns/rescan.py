from ns import nslib

import sys


if __name__ == '__main__':
    if sys.argv[1] == "minute":
        #scan every minute only if free size < 2GB
        if nslib.scan_stats(cached=True)[1]['free'] > 2000:
            sys.exit(0)

    nslib.scan_stats(False)
