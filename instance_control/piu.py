import logging

import subprocess

_LOG = logging.getLogger('bubuku.cluster.piu')


def stop_taupage(ip: str, user: str, odd: str):
    _LOG.info('Stopping taupage container on %s', ip)
    piu = ["piu", ip, "-O", odd, "\"detaching ebs, terminate and launch instance\""]
    _call(piu)

    stop = ["ssh", "-tA", user + '@' + odd, "ssh", "-o", "StrictHostKeyChecking=no", user + '@' + ip,
            "'docker stop -t 300 taupageapp'"]
    _call(stop)
    _LOG.info('Taupage container on %s is successfully stopped', ip)


def _call(cmd):
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    if proc.returncode != 0 and proc.returncode != 64:
        raise Exception(proc.returncode, stderr.decode('utf-8'))
    return stdout
