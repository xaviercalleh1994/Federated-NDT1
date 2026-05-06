# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
import argparse
import time
import psutil
import csv

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pid", type=int, required=True)
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    p = psutil.Process(args.pid)

    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ts_epoch_s", "cpu_percent", "rss_mb", "tx_kbps"])

        last_bytes = p.io_counters().bytes_sent if p.io_counters() else 0
        last_t = time.time()

        while True:
            time.sleep(args.interval)
            now = time.time()

            cpu = p.cpu_percent(interval=None)
            rss = p.memory_info().rss / (1024 * 1024)

            io = p.io_counters()
            if io:
                dbytes = io.bytes_sent - last_bytes
                tx_kbps = (dbytes * 8) / 1000 / (now - last_t)
                last_bytes = io.bytes_sent
                last_t = now
            else:
                tx_kbps = 0.0

            w.writerow([now, cpu, rss, tx_kbps])
            f.flush()

if __name__ == "__main__":
    main()
