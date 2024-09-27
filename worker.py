import threading
import multiprocessing
import traceback
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
            self._worker = threading.Thread(target=self._init_and_run, name=self.name)
            self._init_event = threading.Event()
            self._start_event = threading.Event()
            self._stop_event = threading.Event()
        elif worker_type == WorkerType.Process:
            self._worker = multiprocessing.Process(target=self._init_and_run, name=self.name)
            self._init_event = multiprocessing.Event()
            self._start_event = multiprocessing.Event()
            self._stop_event = multiprocessing.Event()

        self._worker.start()

    def start(self):
        """
        Starts the worker by creating a thread or process and running the `run()` method.
        """
        if not self._start_event.is_set():
            self._init_event.wait()
            self._start_event.set()
        else:
            stack = "".join(traceback.format_stack())
            logger.warning(f"Worker already started. Duplicate call for start().\n{stack}")

    def stop(self):
        """
        Stops the worker if it is running.
        """
        if self._is_not_stopped():
            self._stop_event.set()
            self._worker.join()
        else:
            stack = "".join(traceback.format_stack())
            logger.warning(f"Worker already stopped. Duplicate call for stop().\n{stack}")

    def _is_not_stopped(self) -> bool:
        return not self._stop_event.is_set()

    def _init_and_run(self):
        self._init()
        self._init_event.set()
        logger.info(f"{self.worker_type.value} {self.name} initialized.")

        self._start_event.wait()
        logger.info(f"{self.worker_type.value} {self.name} started.")
        self._run()

        logger.info(f"{self.worker_type.value} {self.name} stopped.")

    @abstractmethod
    def _init(self):
        """
        This method should be implemented by driven classes.
        It defines the init steps before _run call.
        """
        pass

    @abstractmethod
    def _run(self):
        """
        This method should be implemented by driven classes.
        It defines the work the worker will perform.
        """
        pass
