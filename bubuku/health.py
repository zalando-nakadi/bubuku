import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from bubuku.utils import CmdHelper

_LOG = logging.getLogger('bubuku.health')


class _Handler(BaseHTTPRequestHandler):
    cmd_helper = None

    def do_GET(self):
        if self.path in ('/api/disk_stats', '/api/disk_stats/'):
            used_kb, free_kb = self.cmd_helper.get_disk_stats()
            self._send_response({'free_kb': free_kb, 'used_kb': used_kb})
        else:
            self._send_response({'status': 'OK'})

    def _send_response(self, json_):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(json_).encode('utf-8'))


def start_server(port, cmd_helper: CmdHelper) -> threading.Thread:
    def _thread_func():
        _Handler.cmd_helper = cmd_helper
        server = HTTPServer(('', port), _Handler)
        server.serve_forever()
        server.socket.close()

    t = threading.Thread(target=_thread_func, daemon=True)
    _LOG.info('Starting health server on port {}'.format(port))
    t.start()
    return t


if __name__ == '__main__':
    start_server(8080, CmdHelper())
