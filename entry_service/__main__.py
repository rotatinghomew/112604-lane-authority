"""命令行入口：把进区服务作为长期进程运行。

    python3 -m entry_service --db entry.sqlite3 --device-url http://127.0.0.1:8087

启动时可以把合成资料导入状态库（重复导入是幂等的），之后通过 HTTP 接收观测、
纸单、设备回执和值班操作。
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import signal
import sys

from .app import create_app
from .feeds import load_directory, parse_time
from .store import Store, utcnow


def import_samples(service, directory: Path) -> dict[str, int]:
    index = load_directory(directory)
    counts = {"stations": 0, "manifests": 0, "observations": 0, "paper_checks": 0}
    for record in index["stations"]:
        if service.ingest_station(record)["recorded"]:
            counts["stations"] += 1
    for record in index["manifests"]:
        if service.ingest_manifest(record)["recorded"]:
            counts["manifests"] += 1
    for record in index["observations"]:
        if service.ingest_observation(record)["recorded"]:
            counts["observations"] += 1
    for record in index["paper_checks"]:
        if service.ingest_paper_check(record)["recorded"]:
            counts["paper_checks"] += 1
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="entry.sqlite3", help="状态库路径")
    parser.add_argument("--device-url", default="http://127.0.0.1:8087", help="车道设备接口")
    parser.add_argument("--host", default="127.0.0.1", help="仅监听回环地址")
    parser.add_argument("--port", type=int, default=8099, help="监听端口，0 表示随机")
    parser.add_argument("--tick", type=float, default=2.0, help="后台回查间隔（秒）")
    parser.add_argument("--import-samples", default=None, help="启动时导入的合成资料目录")
    parser.add_argument(
        "--now",
        default=None,
        help="把服务时钟固定在此时刻（带 Z 的 ISO 8601），用于回放样本",
    )
    options = parser.parse_args(argv)

    clock = utcnow
    if options.now:
        frozen = parse_time(options.now)
        clock = lambda: frozen
    store = Store(options.db)
    server = create_app(
        store=store,
        device_url=options.device_url,
        tick_interval=options.tick,
        host=options.host,
        port=options.port,
        clock=clock,
    )
    if options.import_samples:
        counts = import_samples(server.service, Path(options.import_samples))
        print(json.dumps({"imported": counts}, ensure_ascii=False), flush=True)

    def stop(signum, frame):
        server._stopped = True
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, stop)
    print(f"进区服务监听 {options.host}:{server.server_port}", flush=True)
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.stop()
        server.server_close()
        store.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
