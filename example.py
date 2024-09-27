import time

from logger import logger
from worker import PipelineWorker, WorkerType


class TestThreadWorker(PipelineWorker):

    def __init__(self) -> None:
        super().__init__(self.__class__.__name__, WorkerType.Thread)

    def _init(self):
        self.buffer = [0] * (1024 * 1024 * 1024)

    def _run(self):
        while self._is_not_stopped():
            logger.info(f"{self.name} is working...")
            time.sleep(1)


class TestProcessWorker(PipelineWorker):

    def __init__(self) -> None:
        print("init")
        self.buffer_before_start = [0] * (1024 * 1024 * 1024)
        super().__init__(self.__class__.__name__, WorkerType.Process)

    def _init(self):
        self.buffer = [0] * (1024 * 1024 * 1024)

    def _run(self):
        while self._is_not_stopped():
            logger.info(f"{self.name} is working...")
            time.sleep(1)


if __name__ == '__main__':
    # worker = TestThreadWorker()
    worker = TestProcessWorker()
    worker.start()
    time.sleep(5)
    worker.stop()
