from subprocess import call


def stop_taupage(ip: str, user: str, odd: str):
    piu = ["piu", ip, "-O", odd, "\"detaching ebs, terminate and launch instance\""]
    call(piu)
    ssh = ["ssh", "-tA", user + '@' + odd, "ssh", "-o", "StrictHostKeyChecking=no", user + '@' + ip,
           "'docker stop taupageapp'"]
    call(ssh)
