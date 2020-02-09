import requests as rq
import logging
import imp

from flask import Flask, escape, request, jsonify
from queue import Queue
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from threading import Thread
from multiprocessing import Process

REQUEST_TIMEOUT = 30

Logger = logging.getLogger()


def import_code(code, name):
    module = imp.new_module('tmp_module')
    exec(code, module.__dict__)
    return module.__dict__[name]

class Kumo:
    thread = None
    result = Queue()
    headers = {"User-Agent": UserAgent().random}
    stop = False

    def _crawl(self, targets, process, stoped):  # this should not throw
        for t in targets:
            if stoped():
                break
            try:
                res = rq.get(t, headers=self.headers, timeout=REQUEST_TIMEOUT)
                soup = BeautifulSoup(res.text, "html.parser")
                self.result.put((t, process(soup)))
            except Exception as e:
                Logger.warning("Request failed: " + str(e))
                self.result.put((t, "Request failed: " + str(e)))

    def working(self):
        return self.thread is not None and self.thread.is_alive()
    
    def crawl(self, targets, processor):
        if self.working():
            return True
        process = import_code(processor, 'process')
        self.thread = Thread(target=self._crawl, args=[targets, process, lambda: self.stop])
        self.thread.start()
        Logger.info("New crawler thread: {}".format(self.thread))
        print(self.thread)
        return False

    def flush_result(self):
        result = {}
        while not self.result.empty():
            t, data = self.result.get()
            result[t] = data
        return result

    def kill(self):
        if self.thread is not None and self.thread.is_alive():
            self.stop = True
            self.thread.join()  # might wait for one cycle to finish
            self.stop = False
        while not self.result.empty():
            self.result.get()


kumo = Kumo()

app = Flask(__name__)

@app.route('/pull', methods=['GET'])
def pull():
    finished = not kumo.working()
    result = kumo.flush_result()
    return jsonify({'finished': finished, 'result': result})

# skinny-dipping
@app.route('/assign', methods=['POST'])
def assign():  # skinny-dip
    data = request.json
    targets = data['targets']
    processor = data['processor']
    already_working = kumo.crawl(targets, processor)
    return jsonify({'already_working': already_working})

@app.route('/isworking', methods=['GET'])
def isworking():
    return jsonify({'result': kumo.working()})

@app.route('/kill', methods=['GET'])
def kill():
    kumo.kill()
    return 'ok'
