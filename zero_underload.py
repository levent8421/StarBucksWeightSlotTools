"""
    清零欠载货道
    并将状态抓取信息存放在 zero_slot_state_out.txt 文件中
    并将清零结果村房子啊 zero_slot_zero_out.txt 文件中
    注意： 运行前请将上述文件清空或删除
"""

from urllib.parse import quote

import requests

from fetch_slot_state import SlotStateFetcher
from store import load_stores
from thread_pool import ThreadPool

_STATE_RES_FILE_NAME = 'zero_slot_state_out.txt'
_ZERO_RES_FILE_NAME = 'zero_slot_zero_out.txt'


def as_store_str(store):
    return 'Store:[%s/%s]%s' % (store.id, store.server_ip, store.name)


def write_msg_into_state_out_file(store, msg):
    store_str = as_store_str(store)
    txt = '%s: %s\n' % (store_str, msg)
    with open(_STATE_RES_FILE_NAME, 'a+') as f:
        f.write(txt)


def write_msg_into_zero_out_file(store, msg):
    store_str = as_store_str(store)
    txt = '%s: %s\n' % (store_str, msg)
    with open(_ZERO_RES_FILE_NAME, 'a+') as f:
        f.write(txt)


class SlotZeroTask:
    def __init__(self, store, slot_no_list):
        self._store = store
        self._slot_no_list = slot_no_list

    def do_zero(self):
        store = self._store
        for slot_no in self._slot_no_list:
            try:
                self._do_one_zero(slot_no)
            except Exception as e:
                write_msg_into_zero_out_file(store, 'Slot[%s] zero error: %s' % (slot_no, repr(e)))

    def _do_one_zero(self, slot_no):
        store = self._store
        server_ip = store.server_ip
        encoded_slot_no = quote(slot_no)
        url = 'http://%s:5601/api/slot/%s/zero' % (server_ip, encoded_slot_no)
        print('%s, Slot[%s] zero......' % (as_store_str(store), slot_no))
        resp = requests.post(url)
        res_json = resp.json()
        if 'code' in res_json and res_json['code'] == 200:
            write_msg_into_zero_out_file(store, 'Slot[%s] zero success!' % slot_no)
        else:
            write_msg_into_zero_out_file(store, 'Slot[%s] zero fail! %s' % (slot_no, res_json['msg']))


class UnderLoadSlotZeroTask:
    def __init__(self, store, total=0, pos=0):
        self._store = store
        self._total = total
        self._pos = pos

    def start(self):
        print('Start task: [%s/%s]' % (self._pos, self._total))
        store = self._store
        store_str = as_store_str(store)
        state_fetcher = SlotStateFetcher(store=store)
        try:
            state_list = state_fetcher.fetch_state()
        except Exception as e:
            print('%s: Can not fetch slot state: %s' % (as_store_str(store), repr(e)))
            write_msg_into_state_out_file(store, 'Fetch state error! %s' % repr(e))
            return
        under_load_slots = []
        for slot_no in state_list:
            if state_list[slot_no] == 5:
                under_load_slots.append(slot_no)
        print('%s UnderLoad Slots: [%s]' % (as_store_str(store), under_load_slots))
        under_load_slot_count = len(under_load_slots)
        if under_load_slot_count <= 0:
            msg = 'No UnderLoad Slot!!!'
            write_msg_into_state_out_file(store, msg)
            return
        print('%s underLoad slot count=%s' % (store_str, under_load_slot_count))
        zero_task = SlotZeroTask(store=store, slot_no_list=under_load_slots)
        zero_task.do_zero()

    def __call__(self, *args, **kwargs):
        self.start()


def start_tasks():
    stores = load_stores()
    thread_pool = ThreadPool(size=20)
    total = len(stores)
    pos = 0
    for store in stores:
        pos += 1
        task = UnderLoadSlotZeroTask(store=store, total=total, pos=pos)
        thread_pool.push_task(task)
    thread_pool.init_pool()
    thread_pool.start()
    print('Waiting for task exit!')
    thread_pool.join()


def main():
    start_tasks()


if __name__ == '__main__':
    main()
