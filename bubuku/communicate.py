import logging
import threading
import time
from queue import Queue, Empty, Full

__COMMAND_QUEUE = Queue()

_LOG = logging.getLogger('bubuku.communicate')


def sleep_and_operate(controller, timeout: float):
    cur_time = time.time()
    finish = cur_time + (0.1 if timeout <= 0 else timeout)
    while cur_time < finish:
        try:
            command = __COMMAND_QUEUE.get(block=True, timeout=finish - cur_time)
            try:
                command(controller)
            except Exception as e:
                _LOG.error('Command finished with error', exc_info=e)
        except Empty:
            pass
        cur_time = time.time()


def execute_on_controller_thread(function, timeout):
    condition = threading.Condition()
    result = [None, True]

    def _execute(controller):
        with condition:
            if result[1]:
                try:
                    result[0] = function(controller)
                finally:
                    condition.notify()

    finish = time.time() + timeout
    with condition:
        try:
            __COMMAND_QUEUE.put(_execute, timeout=timeout)
        except Full:
            raise TimeoutError('Timeout expired')
        if condition.wait(timeout=finish - time.time()):
            return result[0]
        else:
            result[1] = False
            raise TimeoutError('Timeout expired')
