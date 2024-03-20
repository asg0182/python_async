import sys
import time
import yaml
import threading
import requests
import logging
from queue import Queue


logger = logging.getLogger(name="THREAD TASKS")
formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s - %(message)s')
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.INFO)


class Task:
    def __init__(self):
        self.name = None
        self._result = None
        self._exception = None

    def run(self):
        raise NotImplementedError

    def set_result(self, result):
        self._result = result
        self._result['name'] = self.name

    def get_result(self):
        if self._exception:
            raise self._exception
        return self._result

    def set_exception(self, e):
        self._exception = e


class UrlTask(Task):
    def __init__(self, name, job_id):
        super().__init__()
        self.name = name
        self.request_id = None
        self.base_url = "http://127.0.0.1:5000"
        self.job_url = f"{self.base_url}/jobs/{job_id}"
        self.start()
        self.status_url = f"{self.base_url}/requests/{self.request_id}"

    def start(self):
        resp = requests.post(url=self.job_url)
        if resp.ok:
            self.request_id = resp.json().get("id")
            logger.info(f"{self.job_url} - {self.request_id}")
        else:
            raise Exception("Could not trigger the job")

    def run(self):
        status = "running"
        while status != "success":
            time.sleep(1)
            resp = requests.get(url=self.status_url)
            if not resp.ok:
                logger.info(f"{resp.status_code} {resp.text}")
            data = resp.json()
            logger.info(f"{data}")
            status = data["status"]
            if status == "success":
                self.set_result(result=data)
                break
        return

class TaskExecutor:
    def __init__(self, max_workers=4):
        self.task_queue = Queue()
        self.result_queue = Queue()
        self.workers = []
        self.max_workers = max_workers
        self._stopped = False

    def submit_task(self, task):
        if self._stopped:
            raise RuntimeError("Task executor already stopped")
        self.task_queue.put(task)

    def _worker(self):
        while True:
            if self._stopped:
                break
            task = self.task_queue.get()
            try:
                task.run()
            except Exception as e:
                task.set_exception(e)

            self.result_queue.put(task)
            self.task_queue.task_done()

    def start(self):
        for _ in range(self.max_workers):
            worker = threading.Thread(target=self._worker)
            worker.daemon = True
            worker.start()
            self.workers.append(worker)

    def wait_for_completion(self):
        self.task_queue.join()
        results = []
        while not self.result_queue.empty():
            task = self.result_queue.get()
            results.append(task.get_result())
        return results

    def stop(self):
        self._stopped = True
        for worker in self.workers:
            worker.join(timeout=3)


if __name__ == "__main__":
    executor = TaskExecutor()
    with open("tasks.yml", "r") as f:
        tasks_data = yaml.safe_load(f)

    for task_data in tasks_data:
        executor.submit_task(UrlTask(**task_data))

    executor.start()
    results = executor.wait_for_completion()
    executor.stop()

    for result in results:
        logger.info(f" Result: {result}")
