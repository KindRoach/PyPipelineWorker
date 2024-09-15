import threading
import multiprocessing
from abc import ABC, abstractmethod
from enum import Enum

from logger import logger


class WorkerType(Enum):
    Thread = "Thread"
    Process = "Process"


class PipelineWorker(ABC):
    def __init__(self, name: str, worker_type: WorkerType) -> None:
        self.name = name
        self.worker_type = worker_type
        self._running = False

        if worker_type == WorkerType.Thread:
            self._worker = threading.Thread(target=self._run_with_log, name=self.name)
            self._stop_event = threading.Event()
        elif worker_type == WorkerType.Process:
            self._worker = multiprocessing.Process(target=self._run_with_log, name=self.name)
            self._stop_event = multiprocessing.Event()

        self._stop_event.set()

    @abstractmethod
    def run(self):
        """
        This method should be implemented by driven classes.
        It defines the work the worker will perform.
        """
        pass

    def start(self):
        """
        Starts the worker by creating a thread or process and running the `run()` method.
        """
        if self._stop_event is None or self._worker is None:
            raise ValueError("_stop_event and _worker should be init before use them.")

        if not self._is_running():
            self._stop_event.clear()
            self._worker.start()

    def _run_with_log(self):
        logger.info(f"{self.worker_type.value} {self.name} started.")
        self.run()
        logger.info(f"{self.worker_type.value} {self.name} finished.")

    def _is_running(self) -> bool:
        return not self._stop_event.is_set()

    def stop(self):
        """
        Stops the worker if it is running.
        """
        if self._stop_event is None or self._worker is None:
            raise ValueError("_stop_event and _worker should be init before use them.")

        if self._is_running():
            self._stop_event.set()
            self._worker.join()
