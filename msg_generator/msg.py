#!/usr/bin/python
# -*- coding: utf-8 -*-


import argparse
import redis
import threading
import time
import signal
from random import randrange
import logging


def completion_handler(signum, frame):
    pass


def generate_message(number):
    return "{number}:message-{rand}".format(number=number,
                                            rand=randrange(1, 101))


def generate_appname():
    from socket import gethostname
    from os import getpid
    return "{host}-{pid}".format(host=gethostname(), pid=getpid())


class MsgGenerator(threading.Thread):
    def __init__(self, app):
        threading.Thread.__init__(self)
        self.app = app
        self.start_ts = self.app.rdb.get("start")
        if self.start_ts:
            self.start_ts = float(self.start_ts.decode("utf-8"))
        self.gen_lock = self.app.rdb.lock("gen_lock", 1)
        self.cur_msg_number = 1
        last_message = self.app.rdb.get("last_message")
        if last_message:
            last_message = last_message.decode("utf-8")
            (last_number, last_cur_ts) = last_message.split(":")
            self.cur_msg_number = int(last_number)

    def run(self):
        if self.cur_msg_number == self.app.msg_count + 1:
            return
        for i in range(self.cur_msg_number, self.app.msg_count + 1):
            with self.gen_lock:
                cur_ts = time.time()
                if not self.start_ts:
                    self.start_ts = cur_ts
                    self.app.rdb.set("start", self.start_ts)
                logging.debug("Message TimeStamp. Number: {n}. TS: {t}.".format(
                                n=i, t=self.start_ts + self.app.interval * i))
                st_iter_time = time.time()
                gen_name = self.app.rdb.get("generator").decode("utf-8")
                if gen_name != self.app.name:
                    return
                msg_info = "{number}:{timestamp}".format(
                                number=i, timestamp=cur_ts)
                self.app.rdb.set("last_message", msg_info)
                msg = generate_message(i)
                self.app.rdb.rpush("queue", msg)
                print("The generator: {name}. Time: {ts}. "
                      "Generated: {msg}".format(
                        name=self.app.name, ts=cur_ts, msg=msg))
                delay = self.start_ts + self.app.interval * i - cur_ts
                logging.debug("Generator. Number: {n}. Delay: {d}.".format(
                                n=i, d=delay))
                if delay > 0.0:
                    time.sleep(delay)
                logging.debug("Generator. Iteration Time: {}".format(
                                time.time() - st_iter_time))
        self.app.rdb.set("last_message", "{n}:".format(n=self.app.msg_count+1))


class MsgAcceptor(threading.Thread):
    def __init__(self, app):
        threading.Thread.__init__(self)
        self.app = app
        self.accept_lock = self.app.rdb.lock("accept_lock", 2)

    def process(self, text):
        st = time.time()
        parts = text.split("-")
        if int(parts[1]) <= 5:
            self.app.rdb.rpush("errors", text)
        else:
            time.sleep(self.app.interval)
        return time.time() - st

    def run(self):
        while True:
            try:
                with self.accept_lock:
                    st = self.app.rdb.get("start")
                    if not st:
                        self.app.run_generator()
                    msg = self.app.rdb.blpop("queue", 1)
                    if msg:
                        accept_ts = time.time()
                        (index, text) = msg[1].decode("utf-8").split(":")
                        duration = self.process(text)
                        print("The app: {name}. Index: {ix}. Accepted: {msg}. "
                              "Time: {ts}. Process Duration: {dur}.".format(
                                    name=self.app.name, ix=index, ts=accept_ts,
                                    msg=text, dur=duration))
                    last_message = self.app.rdb.get("last_message")
                    process_time = 0
                    st = self.app.rdb.get("start")
                    if not st:
                        continue
                    st = float(st.decode("utf-8"))
                    if last_message:
                        last_message = last_message.decode("utf-8")
                        (last_number, last_cur_ts) = last_message.split(":")
                        last_number = int(last_number)
                        if last_number == self.app.msg_count + 1:
                            if msg:
                                print("Last Number!")
                                continue
                            else:
                                print("No Message.")
                                break
                        process_time = (last_number - 1) * self.app.interval
                    expected_ts = st + process_time
                    delta = time.time() - expected_ts
                    logging.debug("Delta: {d}. Max Interval: {m}.".format(
                                        d=delta, m=self.app.max_interval))
                    if delta > self.app.max_interval:
                        self.app.run_generator()
            except Exception as exc:
                print("Exception: {}".format(exc))


class App(object):
    def __init__(self, interval, max_interval, nmsg, rhost, rport):
        self.name = generate_appname()
        self.msg_count = nmsg
        self.interval = interval
        self.max_interval = max_interval
        self.rdb = redis.Redis(host=rhost, port=rport)
        self.signal_handlers = {
            signal.SIGINT: None,
            signal.SIGQUIT: None,
            signal.SIGTERM: None,
            signal.SIGTSTP: None
        }
        self.disable_completion()
        self.gen_lock = self.rdb.lock("gen_lock", 1)
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
        try:
            with self.gen_lock:
                st = time.time()
                logging.debug("Acceptor. Start Gen Lock: {t}".format(t=st))
                gen_name = self.rdb.get("generator")
                if gen_name:
                    gen_name = gen_name.decode("utf-8")
                else:
                    self.rdb.set("generator", self.name)
                if self.name == gen_name:
                    return
                self.rdb.set("generator", self.name)
                logging.debug("Acceptor. End Gen Lock. "
                              "Duration: {dur}".format(
                                dur=time.time()-st))
        except redis.exceptions.LockError as exc:
            print("Redis Exception: {}".format(exc))
            return
        gen = MsgGenerator(self)
        gen.start()


def main():
    logging.basicConfig(
        filename="/tmp/msg_gen.{}.log".format(generate_appname()),
        level=logging.DEBUG)
    parser = argparse.ArgumentParser(
                description="Тестовое задание в OneTwoTrip.")
    parser.add_argument("-c", "--command",
                        default="handle", type=str,
                        choices=["handle", "getErrors", "clean"],
                        help="Обработать сообщение.")
    parser.add_argument("-i", "--interval", default=500, type=int,
                        help="Интервал между сообщениями (мс).")
    parser.add_argument("-m", "--max-interval", default=900, type=int,
                        help="Maксимальный интервал между сообщениями (мс).")
    parser.add_argument("-n", "--number", default=100, type=int,
                        help="Количество генерируемых сообщений.")
    parser.add_argument("-t", "--host", default="localhost", type=str,
                        help="Имя хоста с redis.")
    parser.add_argument("-p", "--port", default=6379, type=str,
                        help="Порт, на котором redis принимает соединения.")
    args = parser.parse_args()

    if args.command == "handle":
        App(args.interval/1000, args.max_interval/1000,
            args.number, args.host, args.port)
    elif args.command == "getErrors":
        rdb = redis.Redis(host=args.host, port=args.port)
        nerrors = rdb.llen("errors")
        i = 1
        while nerrors:
            message = rdb.lpop("errors")
            if message:
                message = message.decode("utf-8")
            print("{number}. Message with error: {msg}".format(number=i,
                                                               msg=message))
            nerrors -= 1
            i += 1
    elif args.command == "clean":
        rdb = redis.Redis(host=args.host, port=args.port)
        rdb.delete("generator")
        rdb.delete("queue")
        rdb.delete("last_message")
        rdb.delete("start")
        rdb.delete("gen_lock")
        rdb.delete("accept_lock")


if __name__ == "__main__":
    main()
