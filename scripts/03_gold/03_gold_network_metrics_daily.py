"""Standard spark-submit wrapper for the daily Gold stage."""

import sys

from network_metrics.main import main


if __name__ == "__main__":
    if "--stage" not in sys.argv:
        sys.argv.extend(["--stage", "gold-daily"])
    main()
