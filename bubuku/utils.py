import subprocess


class CmdHelper:
    def cmd_run(self, cmd):
        output = subprocess.check_output(cmd, shell=True)
        return output.decode("utf-8")
