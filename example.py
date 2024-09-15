import time

from logger import logger
from worker import PipelineWorker, WorkerType


class TestThreadWorker(PipelineWorker):

    def __init__(self) -> None:
        super().__init__(self.__class__.__name__, WorkerType.Thread)

    def run(self):
        while self._is_running():
            logger.info(f"{self.name} is working...")
            time.sleep(1)


if __name__ == '__main__':
    worker = TestThreadWorker()
    worker.start()
    time.sleep(5)
    worker.stop()
