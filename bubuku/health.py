import json
import logging
import threading
from functools import partial
from http.server import BaseHTTPRequestHandler, HTTPServer

from bubuku.communicate import execute_on_controller_thread
from bubuku.controller import Controller
from bubuku.utils import CmdHelper

_CONTROLLER_TIMEOUT = 5

_API_CONTROLLER = '/api/controller/'

_LOG = logging.getLogger('bubuku.health')


def load_controller_queue(controller: Controller):
    return controller.enumerate_changes()


def delete_from_controller_queue(name: str, controller: Controller):
    return {
        'count': controller.cancel_changes(name)
    }


class _Handler(BaseHTTPRequestHandler):
    cmd_helper = None

    def do_GET(self):
        if self.path in ('/api/disk_stats', '/api/disk_stats/'):
            used_kb, free_kb = self.cmd_helper.get_disk_stats()
            self._send_response({'free_kb': free_kb, 'used_kb': used_kb})
        elif self.path.startswith(_API_CONTROLLER):
            self.wrap_controller_execution(lambda: self._run_controller_action(self.path[len(_API_CONTROLLER):]))
        else:
            self._send_response({'status': 'OK'})

    def wrap_controller_execution(self, call):
        try:
            call()
        except TimeoutError as e:
            _LOG.error('Failed to rum action because of timeouts', exc_info=e)
            self._send_response({'code': 'timeout', 'message': 'Timeout occurred'}, 500)

    def do_DELETE(self):
        if not self.path.startswith(_API_CONTROLLER):
            return self._send_response({'message': 'Path {} is not supported'.format(self.path)}, 404)
        action = self.path[len(_API_CONTROLLER):].split('/')
        if action[0] == 'queue':
            if len(action) < 2:
                return self._send_response({'message': 'No second argument provided!'}, 400)
            self.wrap_controller_execution(
                lambda: self._send_response(execute_on_controller_thread(
                    partial(delete_from_controller_queue, action[1]), _CONTROLLER_TIMEOUT), 200))
        else:
            return self._send_response({'message': 'Action {} is not supported'.format(action[0])}, 404)

    def _run_controller_action(self, action):
        if action.split('/')[0] == 'queue':
            return self._send_response(execute_on_controller_thread(load_controller_queue, _CONTROLLER_TIMEOUT), 200)
        else:
            return self._send_response({'message': 'Action {} is not supported'.format(action)}, 404)

    def _send_response(self, json_, status_code=200):
        self.send_response(status_code)
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
