import subprocess


class CmdHelper(object):
    def get_disk_stats(self) -> (int, int):
        """
        Returns total disk stats,
        :return: used_kb, free_kb
        """
        disks = self.cmd_run("df -k | tail -n +2 |  awk '{ print $3, $4 }'").split("\n")
        total_used = total_free = 0
        for disk in disks:
            parts = disk.split(" ")
            if len(parts) == 2:
                used, free = tuple(parts)
                total_used += int(used)
                total_free += int(free)
        return total_used, total_free

    def cmd_run(self, cmd):
        output = subprocess.check_output(cmd, shell=True)
        return output.decode("utf-8")
