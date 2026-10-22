import http.server
import sys
from pathlib import Path

ALLOWED_UNITS = {"bench-sirannon.service", "bench-postgres.service"}
CGROUP_ROOT = Path("/sys/fs/cgroup/system.slice")


class CpuStatHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        unit = self.path.lstrip("/")
        if unit not in ALLOWED_UNITS:
            self.send_error(404)
            return
        try:
            body = (CGROUP_ROOT / unit / "cpu.stat").read_bytes()
        except OSError:
            self.send_error(503)
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> None:
    host, port = sys.argv[1], int(sys.argv[2])
    http.server.ThreadingHTTPServer((host, port), CpuStatHandler).serve_forever()


if __name__ == "__main__":
    main()
