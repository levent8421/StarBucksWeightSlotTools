"""
    关闭蠕变补偿
    并将结果存放在 disable_compensation_out.txt 文件中
    注意 运行前请将上述文件清空或删除
"""
import requests

from store import load_stores
from thread_pool import ThreadPool

DISABLE_RETRY = 3
OUTPUT_FILE_NAME = 'disable_compensation_out.txt'

REQUEST_HEADER = {'Content-Type': 'application/json;charset=utf-8'}


def write_msg_into_file(store, msg):
    msg = '[%s/%s]:%s\t%s\n' % (store.id, store.server_ip, store.name, msg)
    with open(OUTPUT_FILE_NAME, 'a+') as f:
        f.write(msg)


class CompensationDisableTask:
    def __init__(self, store, index=0, total=0):
        self._store = store
        self._index = index
        self._total = total

    def __call__(self, *args, **kwargs):
        store = self._store
        print('Processing[%d/%d]: [%s/%s]:[%s]' % (self._index, self._total, store.server_ip, store.id, store.name))
        for _ in range(DISABLE_RETRY):
            try:
                self._do_disable()
            except Exception as e:
                error_msg = 'Error: %s' % repr(e)
                write_msg_into_file(store, error_msg)

    def _do_disable(self):
        store = self._store
        url = 'http://%s:5601/api/slot/_all-compensation' % store.server_ip
        body_json = {'enableCompensation': False}
        resp = requests.post(url, headers=REQUEST_HEADER, json=body_json)
        resp_body = resp.json()
        if 'code' in resp_body and resp_body['code'] == 200:
            write_msg_into_file(store, 'Disable OK')
        else:
            write_msg_into_file(store, 'Disable error:[%s/%s]' % (resp_body['code'], resp_body['msg']))


def main():
    store_list = load_stores()
    thread_pool = ThreadPool(size=20)
    index = 0
    total = len(store_list)
    for store in store_list:
        index += 1
        task = CompensationDisableTask(store=store, index=index, total=total)
        thread_pool.push_task(task)
    thread_pool.init_pool()
    print('Starting tasks...')
    thread_pool.start()
    print('Waiting for task exit!')
    thread_pool.join()


if __name__ == '__main__':
    main()
