"""
    抓取传感器错误信息
    并将结果存放在 sensor_state_errors.txt 文件中
    注意 运行前请将上述文件清空或删除
"""
import requests

from store import load_stores, Store
from thread_pool import ThreadPool

ERROR_FILE_NAME = 'sensor_state_errors.txt'


class ErrorFetcher:
    def as_error_str(self, errors):
        store = self._store
        error_count = len(errors)
        error_msg_list = ['Slot:[%s],msg:[%s]' % (error['slotNo'], error['message']) for error in errors]
        return 'Store:[%s], errorCount:[%d]:%s\t' % (store, error_count, '\t'.join(error_msg_list))

    def as_exception_str(self, ex):
        store = self._store
        ex_str = repr(ex)
        return 'Store:[%s], error:[%s]' % (store, ex_str)

    def __init__(self, store):
        self._store = store

    def fetch_error_into_file(self, file_name):
        store = self._store
        ip = store.server_ip
        url = 'http://%s:5601/api/status/_errors' % ip
        try:
            resp = requests.get(url)
        except Exception as e:
            error_str = self.as_exception_str(e)
            self.append_info_into_file(error_str, file_name)
            return
        try:
            json_res = resp.json()
        except Exception as e:
            error = self.as_exception_str(e)
            self.append_info_into_file(error, file_name)
            return
        errors = json_res['data']
        error_count = len(errors)
        if error_count > 0:
            print(error_count, errors)
            error_str = self.as_error_str(errors)
            ErrorFetcher.append_info_into_file(error_str, file_name)

    @staticmethod
    def append_info_into_file(msg, file_name):
        print('Append: [%s] into [%s]' % (msg, file_name))
        with open(file_name, 'a+') as f:
            f.write(msg)
            f.write('\n')


class FetcherTask:
    def __init__(self, store, num, total):
        self._store = store
        self._num = num
        self._total = total

    def __call__(self, *args, **kwargs):
        store = self._store
        num = self._num
        total = self._total
        fetcher = ErrorFetcher(store)
        print('Fetch error from [%s], [%d/%d]' % (store, num, total))
        fetcher.fetch_error_into_file(ERROR_FILE_NAME)

    @property
    def name(self):
        return 'Fetcher:[%s]' % self._store


def start_tasks():
    thread_pool = ThreadPool(size=20)
    store_list = load_stores()
    total_count = len(store_list)
    count = 0
    for store in store_list:
        count += 1
        task = FetcherTask(store=store, num=count, total=total_count)
        thread_pool.push_task(task)
    thread_pool.init_pool()
    thread_pool.start()
    print('Waiting Task Finished......')
    thread_pool.join()


def run_test():
    store = Store()
    store.server_ip = '10.233.93.82'
    store.id = '12'
    store.name = 'test'
    fetcher = FetcherTask(store=store, total=1, num=1)
    fetcher()


def main():
    run_test()
    # start_tasks()


if __name__ == '__main__':
    main()
