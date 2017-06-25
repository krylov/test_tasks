#!/usr/bin/python
# -*- coding: utf-8 -*-


import argparse
import redis
import threading
import time
import signal
from random import randrange


def completion_handler(signum, frame):
    pass


def generate_message(number):
    return "{number}:message-{rand}".format(number=number,
                                            rand=randrange(1, 101))


def generate_appname():
    from socket import gethostname
    from os import getpid
    return "{host}#{pid}".format(host=gethostname(), pid=getpid())


class MsgGenerator(threading.Thread):
    def __init__(self, app):
        threading.Thread.__init__(self)
        self.app = app
        self.start_ts = self.app.rdb.get("start")
        if self.start_ts:
            self.start_ts = float(self.start_ts)

    def run(self):
        gen_name = self.app.rdb.get("generator").decode("utf-8")
        if gen_name != self.app.name:
            return
        last_message = self.app.rdb.get("last_message")
        cur_msg_number = 1
        if last_message:
            (last_number, last_cur_ts) = str(last_message).split(":")
            cur_msg_number = int(last_number)
        cur_ts = time.time()
        # проверяем, установлено ли время начала генерации сообщений
        # если нет, устанавливаем
        if not self.start_ts:
            self.start_ts = cur_ts
            self.app.rdb.set("start", self.start_ts)
        for i in range(cur_msg_number, self.app.msg_count + 1):
            msg_info = "{number}:{timestamp}".format(
                            number=i, timestamp=cur_ts)
            self.app.rdb.set("last_message", msg_info)
            msg = generate_message(i)
            self.app.rdb.rpush("queue", msg)
            print("The generator: {name}. Time: {ts}. Generated: {msg}".format(
                    name=self.app.name, ts=cur_ts, msg=msg))
            delay = self.start_ts + self.app.interval * i - cur_ts
            if delay > 0.0:
                time.sleep(delay)
            cur_ts = time.time()


class MsgAcceptor(threading.Thread):
    def __init__(self, app):
        threading.Thread.__init__(self)
        self.app = app

    def run(self):
        st = self.app.rdb.get("start")
        if not st:
            self.app.run_generator()
        while True:
            last_message = self.app.rdb.get("last_message")
            if not last_message:
                continue
            last_message = last_message.decode("utf-8")
            st = float(self.app.rdb.get("start"))
            (last_number, last_cur_ts) = last_message.split(":")
            expected_ts = st + (int(last_number) - 1) * self.app.interval
            if time.time() - expected_ts > self.app.max_interval:
                self.app.run_generator()
            msg = self.app.rdb.blpop("queue")
            print("The app: {name}. Accepted: {msg}".format(
                        name=self.app.name,
                        msg=msg[1].decode("utf-8")))


class App(object):
    def __init__(self, interval, max_interval, nmsg, rhost, rport):
        self.name = generate_appname()
        self.msg_count = nmsg
        self.interval = interval
        self.max_interval = max_interval
        self.rdb = redis.Redis(host=rhost, port=rport)
        self.signal_handlers = {
            signal.SIGINT: None,
            # signal.SIGKILL: None,
            signal.SIGQUIT: None,
            signal.SIGTERM: None,
            signal.SIGTSTP: None
        }
        self.disable_completion()
        self.acceptor = MsgAcceptor(self)
        self.acceptor.start()
        print("The '{name}' app started.".format(name=self.name))

    def disable_completion(self):
        for s in self.signal_handlers:
            self.signal_handlers[s] = signal.signal(s, completion_handler)

    def enable_completion(self):
        for s in self.signal_handlers:
            if not self.signal_handlers[s]:
                signal.signal(s, self.signal_handlers[s])
                self.signal_handlers[s] = None

    def get_last_message(self):
        lst = self.app.rdb.lrange("queue", -1, -1)
        if not lst:
            return {}
        parts = str(lst[0]).split(":")
        msg = {}
        msg["timestamp"] = float(parts[0])
        msg["index"] = int(parts[1])
        msg["text"] = str(parts[2])
        return msg

    def run_generator(self):
        status = self.rdb.get("gen")
        if not status:
            status = "unknown"
        else:
            status = str(status)
        if status in ["unknown", "working"]:
            self.rdb.set("gen", "starting")
        else:
            return
        self.rdb.set("generator", self.name)
        gen = MsgGenerator(self)
        gen.start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                description="Тестовое задание в OneTwoTrip.")
    parser.add_argument("-c", "--command",
                        default="handle",
                        choices=["handle", "getErrors", "clean"],
                        help="Обработать сообщение.")
    parser.add_argument("-i", "--interval", default=500,
                        help="Интервал между сообщениями (мс).")
    parser.add_argument("-m", "--max-interval", default=900,
                        help="Maксимальный интервал между сообщениями (мс).")
    parser.add_argument("-n", "--number", default=100,
                        help="Количество генерируемых сообщений.")
    parser.add_argument("-t", "--host", default="localhost",
                        help="Имя хоста с redis.")
    parser.add_argument("-p", "--port", default=6379,
                        help="Порт, на котором redis принимает соединения.")
    args = parser.parse_args()

    if args.command == "handle":
        app = App(args.interval/1000, args.max_interval/1000,
                  args.number, args.host, args.port)
    elif args.command == "getErrors":
        print("It's need to show errors.")
    elif args.command == "clean":
        rdb = redis.Redis(host=args.host, port=args.port)
        rdb.delete("gen")
        rdb.delete("generator")
        rdb.delete("queue")
        rdb.delete("last_message")
        rdb.delete("start")
