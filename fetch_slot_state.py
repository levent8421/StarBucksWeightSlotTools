"""
    抓取货道错误信息
    并将状态抓取信息存放在 store_slot_state_out.txt 文件中
    注意： 运行前请将上述文件清空或删除
"""

import requests

from store import load_stores
from thread_pool import ThreadPool

STATE_MAPPING = {
    1: 'OK',
    2: 'offline',
    3: 'disable',
    4: 'overload',
    5: 'under_load',
    6: 'merged',
}
_RESULT_FILE_NAME = 'store_slot_state_out.txt'

ERROR_STATE = [2, 4, 5]


def write_msg_into_result_file(store, msg):
    txt = 'Store:[%s]:\t%s\n' % (store.__str__(), msg)
    with open(_RESULT_FILE_NAME, 'a+') as f:
        f.write(txt)


def state_str(state):
    if state in STATE_MAPPING:
        return STATE_MAPPING[state]
    return 'Unknown state: [%s]' % state


class SlotStateFetcher:
    def __init__(self, store):
        self._store = store

    def fetch_state(self):
        store = self._store
        url = 'http://%s:5601/api/dashboard/_data' % store.server_ip
        resp = requests.get(url)
        res_json = resp.json()
        slot_state_table = {}
        if 'code' in res_json and res_json['code'] == 200:
            slots = res_json['data']['slotData']
            for slot_no in slots:
                slot = slots[slot_no]
                state = slot['state']
                slot_state_table[slot_no] = state
                if 'sensors' in slot:
                    sensors = slot['sensors']
                    if sensors is None or len(sensors) <= 0:
                        slot_state_table[slot_no] = 6
        return slot_state_table


class SlotStateFetchTask:
    def __init__(self, store, total=0, pos=0):
        self._store = store
        self._total = total
        self._pos = pos
        self._fetcher = None

    def start(self):
        store = self._store
        print('Fetch state: [%s/%s]\t[%s]' % (self._pos, self._total, store.__str__))
        self._fetcher = SlotStateFetcher(store=store)
        self._do_fetch_state()

    def _do_fetch_state(self):
        try:
            status = self._fetcher.fetch_state()
        except Exception as e:
            msg = 'Error: %s' % repr(e)
            write_msg_into_result_file(self._store, msg)
            return
        err_slots = []
        for slot_no in status:
            state = status[slot_no]
            if state in ERROR_STATE:
                state_msg = state_str(state)
                err_slots.append('[%s: %s]\t' % (slot_no, state_msg))
        if len(err_slots) <= 0:
            return
        msg = ','.join(err_slots)
        write_msg_into_result_file(self._store, msg)

    def __call__(self, *args, **kwargs):
        self.start()


def main():
    store_list = load_stores()
    thread_pool = ThreadPool(size=20)
    pos = 0
    total = len(store_list)
    for store in store_list:
        pos += 1
        task = SlotStateFetchTask(store, pos=pos, total=total)
        thread_pool.push_task(task)
    thread_pool.init_pool()
    thread_pool.start()
    print('Waiting for tasks exit!!!')
    thread_pool.join()


if __name__ == '__main__':
    main()
