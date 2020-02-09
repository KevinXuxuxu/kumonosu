import sys
import requests as rq
import logging
import inspect
from collections import defaultdict
from time import sleep

logging.basicConfig(format='%(asctime)-15s  %(message)s', level=logging.INFO)
Logger = logging.getLogger('KumoDriver')

# Worker status
IDLE = 'IDLE'
INIT_FAILED = 'INIT_FAILED'
INIT = 'INIT'
WAIT = 'WAIT'
WORKING = 'WORKING'
UNHEALTHY = 'UNHEALTHY'

# Job status
UNVISITED = 0
TAKEN = 1
FINISHED = 2

WORKER_ROTATE_TIME = 3  # sec
WORKER_PULL_TIMEOUT = 10  # sec
WORKER_UNHEALTHY_THRESHOLD = 5

class KumoWorker:

    def __init__(self, _id):
        self.id = _id
        self.endpoint = None
        self.status = IDLE
        self.job = None
        self.fail_count = 0
    
    def _fail(self, msg):
        Logger.warning(msg)
        self.fail_count += 1
        if self.fail_count == 5:
            Logger.warning("Worker-{} {} is unhealthy!".format(self.id, self.endpoint))
            self.fail_count = 0
            self.status = UNHEALTHY
    
    def _call(self, method, route, data=None):
        try:
            res = rq.request(method, self.endpoint + route, json=data, timeout=WORKER_PULL_TIMEOUT)
        except Exception as e:
            self._fail("Exception when pulling from {}\n\t\t{}".format(self.endpoint, e))
            return None, False
        if res.status_code != 200:
            self._fail("Pull from {} failed with status code {}".format(
                self.endpoint, res.status_code))
            return None, False
        return res, True

    def init(self):
        # TODO: start worker in AWS EC2 instance
        self.endpoint = 'http://localhost:808{}'.format(self.id)
        self.status = INIT
        return True

    def pull(self):
        res, successful = self._call('GET', '/pull')
        if not successful:
            return None, False
        result = res.json()
        if 'finished' not in result or 'result' not in result \
                or not isinstance(result['result'], dict):
            self._fail("Pull from {} returned with wrong format:\n\t\t{}".format(
                self.endpoint, result))
        if result['finished']:
            self.status = WAIT
        return result, True

    def assign(self, job):
        res, successful = self._call('POST', '/assign', data=job)
        if successful:
            self.status = WORKING
            if res.json()['already_working']:
                Logger.warning('Worker already working!')
                return False
        self.job = job
        return successful

    def kill(self):
        _, successful = self._call('GET', '/kill')
        if successful:
            self.status = WAIT


# TODO: Move to cloud based KVStore
class TempKVStore:
    d = defaultdict(int)

    def available(self, k):
        return self.d[k] == UNVISITED
    
    def taken(self, k):
        return self.d[k] == TAKEN
    
    def finished(self, k):
        return self.d[k] == FINISHED

    def take(self, k):
        self.d[k] = TAKEN

    def put_back(self, k):
        self.d[k] = UNVISITED

    def finish(self, k):
        self.d[k] = FINISHED


class KumoMaster:

    def __init__(self, pool_size, targets, processor, chunk_size=5, output='result.csv', flat=False):
        self.targets = targets
        self.target_pointer = 0
        self.chunk_size = chunk_size
        self.processor = self._func_to_code(processor)
        self.flat = flat

        self.pool_size = pool_size
        self.workers = []
        self.check_interval = WORKER_ROTATE_TIME / self.pool_size

        self.kv_store = TempKVStore()

        self.file = open(output, 'w')
        
        for i in range(self.pool_size):
            self.workers.append(KumoWorker(i))
        
        # Initializing workers
        for w in self.workers:
            w.init()

    def _get_job(self):
        job = []
        for t in self.targets:
            if self.kv_store.available(t):
                self.kv_store.take(t)
                job.append(t)
            if len(job) == self.chunk_size:
                break
        return {'targets': job, 'processor': self.processor}

    def _put_back(self, job):
        for t in job:
            if self.kv_store.taken(t):
                self.kv_store.put_back(t)

    def _output(self, target, result):
        if self.flat:
            for r in result:
                self.file.write("{},{}\n".format(target, r))
                self.file.flush()
        else:
            self.file.write("{},{}\n".format(target, result))
            self.file.flush()

    def _func_to_code(self, func):
        return inspect.getsource(func).strip()

    def _finished(self):
        return all(self.kv_store.finished(t) for t in self.targets)
    
    def run(self):
        while True:
            if self._finished():
                Logger.info("Crawling finished!")
                break
            for w in self.workers:
                Logger.info("worker {} in status {}".format(w.endpoint, w.status))
                if w.status in [INIT_FAILED]:
                    # TODO: handle these cases
                    pass
                elif w.status == IDLE:
                    if w.init():
                        w.status = INIT
                elif w.status in [INIT, WAIT]:
                    job = self._get_job()
                    if job['targets'] != []:
                        ok = w.assign(job)
                        if not ok:
                            self._put_back(job['targets'])
                elif w.status == WORKING:
                    response, successful = w.pull()
                    if successful:
                        for t in response['result']:
                            self._output(t, response['result'][t])
                            self.kv_store.finish(t)
                        if response['finished']:
                            self._put_back(w.job['targets'])
                    if w.status == UNHEALTHY:
                        self._put_back(w.job['targets'])
                elif w.status == UNHEALTHY:
                    w.kill()
                else:
                    Logger.warning("Unrecognized worker status: " + w.status)
                sleep(self.check_interval)
        self.file.close()
