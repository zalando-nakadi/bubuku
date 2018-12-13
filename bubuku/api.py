import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from bubuku.utils import get_opt_broker_id, prepare_configs
from bubuku.zookeeper import load_exhibitor_proxy


class ApiConfig():
    ENDPOINT = '/api/broker/'
    PORT = 8888

    @staticmethod
    def get_url(ip: str, path: str):
        return 'http://{}:{}{}{}'.format(ip, ApiConfig.PORT, ApiConfig.ENDPOINT, path)


_LOG = logging.getLogger('bubuku.api')


class _Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path in '{}{}'.format(ApiConfig.ENDPOINT, 'stop'):
            config, env_provider = prepare_configs()
            with load_exhibitor_proxy(env_provider.get_address_provider(), config.zk_prefix) as zk:
                try:
                    broker_id = get_opt_broker_id(None, config, zk, env_provider)
                    from bubuku.features.remote_exec import RemoteCommandExecutorCheck
                    RemoteCommandExecutorCheck.register_stop(zk, broker_id)
                    self._send_response()
                except Exception as e:
                    self._send_response({'state': 'stopped', 'details': str(e)})
        if self.path in '{}{}'.format(ApiConfig.ENDPOINT, 'start'):
            config, env_provider = prepare_configs()
            with load_exhibitor_proxy(env_provider.get_address_provider(), config.zk_prefix) as zk:
                try:
                    broker_id = get_opt_broker_id(None, config, zk, env_provider)
                    from bubuku.features.remote_exec import RemoteCommandExecutorCheck
                    RemoteCommandExecutorCheck.register_start(zk, broker_id)
                    self._send_response()
                except Exception as e:
                    self._send_response({'details': str(e)})

    def do_GET(self):
        if self.path in '{}{}'.format(ApiConfig.ENDPOINT, 'state'):
            config, env_provider = prepare_configs()
            with load_exhibitor_proxy(env_provider.get_address_provider(), config.zk_prefix) as zk:
                try:
                    broker_id = get_opt_broker_id(None, config, zk, env_provider)
                    if zk.is_broker_registered(broker_id):
                        if any([c not in zk.get_running_changes() for c in ['start', 'restart', 'stop']]):
                            self._send_response({'state': 'restarting'})
                            return
                        self._send_response({'state': 'running'})
                    else:
                        self._send_response({'state': 'stopped'})
                except Exception as e:
                    self._send_response({'state': 'stopped', 'details': str(e)})

    def _send_response(self, json_=None, status_code=200):
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(json_).encode('utf-8'))


def start_api_server() -> threading.Thread:
    def _thread_func():
        server = HTTPServer(('', ApiConfig.PORT), _Handler)
        server.serve_forever()
        server.socket.close()

    t = threading.Thread(target=_thread_func, daemon=True)
    _LOG.info('Starting API server on port {}'.format(ApiConfig.PORT))
    t.start()
    return t
