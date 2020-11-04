from threading import Thread, current_thread


class TaskWrapper:
    def __init__(self, task):
        self._task = task

    def __call__(self, *args, **kwargs):
        thread_name = current_thread().name
        task = self._task
        if hasattr(task, 'name'):
            task_name = task.name
        else:
            task_name = str(task)
        print('Run task [%s] in thread [%s]' % (task_name, thread_name))
        self._task()


class ThreadPool:
    def __init__(self, size):
        self._tasks = []
        self._threads = []
        self._size = size

    def init_pool(self):
        def thread_task():
            self._do_task()

        num = 0
        while len(self._threads) < self._size:
            num += 1
            name = 'T-%d' % num
            self._threads.append(Thread(target=thread_task, name=name))

    def start(self):
        for thread in self._threads:
            thread.start()

    def _do_task(self):
        while len(self._tasks) > 0:
            task = self._tasks.pop(0)
            task_wrapper = TaskWrapper(task=task)
            task_wrapper()

    def push_task(self, task):
        self._tasks.append(task)

    def join(self):
        for thread in self._threads:
            thread.join()
