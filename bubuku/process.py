import subprocess


class KafkaProcess(object):
    def __init__(self, kafka_dir: str):
        self.process = None
        self.kafka_dir = kafka_dir

    def start(self, settings_file):
        if self.is_running():
            raise Exception('Kafka process already started')
        self.process = subprocess.Popen([self.kafka_dir + "/bin/kafka-server-start.sh", settings_file])

    def is_running(self) -> bool:
        if self.process:
            self.process.poll()
            return self.process.returncode is None
        return False

    def stop_and_wait(self):
        if self.process is None:
            raise Exception('Process was not started')
        self.process.terminate()
        self.process.wait()
        self.process = None
