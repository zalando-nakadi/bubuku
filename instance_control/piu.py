from subprocess import call

import logging

_LOG = logging.getLogger('bubuku.cluster.piu')


def stop_taupage(ip: str, user: str, odd: str):
    _LOG.info('Stopping taupage container on %s', ip)
    piu = ["piu", ip, "-O", odd, "\"detaching ebs, terminate and launch instance\""]
    call(piu)
    _LOG.info('Connected to %s', ip)
    ssh = ["ssh", "-tA", user + '@' + odd, "ssh", "-o", "StrictHostKeyChecking=no", user + '@' + ip,
           "'docker stop taupageapp'"]
    call(ssh)
    _LOG.info('Taupage container on %s successfully stopped', ip)
