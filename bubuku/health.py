import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

_LOG = logging.getLogger('bubuku.health')


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps({'status': 'OK'}).encode('utf-8'))


def start_server(port) -> threading.Thread:
    def _thread_func():
        server = HTTPServer(('', port), _Handler)
        server.serve_forever()
        server.socket.close()

    t = threading.Thread(target=_thread_func, daemon=True)
    _LOG.info('Starting health server on port {}'.format(port))
    t.start()
    return t
