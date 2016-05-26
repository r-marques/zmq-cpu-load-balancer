import os
import time
import zmq
import multiprocessing as mp
import random


class Client(object):

    def __init__(self):
        self.ctx = zmq.Context()

        # socket to send requests to LoadBalancer
        self.balancer_sock = self.ctx.socket(zmq.PUSH)
        self.balancer_sock.connect('tcp://localhost:5556')

    def run(self):
        while True:
            num = random.randint(10, 1000)
            self.balancer_sock.send(str(num).encode())


class Worker(object):

    def __init__(self, name):
        self.name = name
        self.ctx = zmq.Context()

        # socket to receive tasks from the LoadBalancer
        self.balancer_sock = self.ctx.socket(zmq.PULL)
        self.balancer_sock.connect('tcp://localhost:5557')

        # socket to receive commands from the LoadBalancer
        self.balancer_command = self.ctx.socket(zmq.PULL)
        self.balancer_command.connect('tcp://localhost:5558')

        # create a poller
        self.poller = zmq.Poller()
        self.poller.register(self.balancer_sock, zmq.POLLIN)
        self.poller.register(self.balancer_command, zmq.POLLIN)

    def work(self, msg):
        """work to do"""
        num = int(msg)
        num * num

    def run(self):
        print('Worker-{} starting ...'.format(self.name))

        while True:
            evts = dict(self.poller.poll())

            if self.balancer_command in evts:
                print('received command')
                msg = self.balancer_command.recv()
                print(msg)
                break
            if self.balancer_sock in evts:
                msg = self.balancer_sock.recv()
                self.work(msg)

        self.destroy()

    def destroy(self):
        print('Worker-{} exiting ...'.format(self.name))
        # self.balancer_sock.close()
        # self.ctx.destroy()


class LoadBalancer(object):

    def __init__(self, load=0.8):
        self.load = load * mp.cpu_count()
        self.ctx = zmq.Context()

        # socket to read client requests
        self.client_sock = self.ctx.socket(zmq.PULL)
        self.client_sock.bind('tcp://*:5556')

        # socket to push tasks to the workers
        self.worker_sock = self.ctx.socket(zmq.PUSH)
        self.worker_sock.bind('tcp://*:5557')

        # internal socket to send commands to workers
        self.worker_command = self.ctx.socket(zmq.PUSH)
        self.worker_command.bind('tcp://*:5558')

        # initialize a poller
        self.poller = zmq.Poller()
        self.poller.register(self.client_sock, zmq.POLLIN)

        # handle workers
        self.num_workers = 0

        # aimd constants
        self.ADDICTIVE_INCREMENT = 1
        self.MULTIPLICATIVE_DECREASE = 0.3
        self.HYSTERESIS = 0.05 * mp.cpu_count()

        self.balance_interval = 30
        self.balance_at = 0
        # we need to have at least one woker
        self.balance_workers()

        print('starting cpu balancer for a load of {}'.format(self.load))
        print('workers will be created if load < {}'.format(self.load - self.HYSTERESIS))
        print('workers will be destroyed if load > {}'.format(self.load + self.HYSTERESIS))

    def worker_proc(self, name):
        worker = Worker(name)
        worker.run()

    def create_worker(self, n):
        """create n workers"""
        print('creating {} workers'.format(n))
        for _ in range(n):
            wp = mp.Process(target=self.worker_proc, args=(self.num_workers,))
            wp.start()
            self.num_workers += 1

    def destroy_workers(self, n):
        """send n poison pills"""
        print('destroying {} workers'.format(n))
        for _ in range(n):
            if self.num_workers > 1:
                self.worker_command.send(b'stop')
                self.num_workers -= 1

    def balance_workers(self):
        if time.time() > self.balance_at:
            loadavg = os.getloadavg()[0]
            if loadavg < (self.load - self.HYSTERESIS):
                self.create_worker(self.ADDICTIVE_INCREMENT)
            elif loadavg > (self.load + self.HYSTERESIS):
                self.destroy_workers(round(self.num_workers * self.MULTIPLICATIVE_DECREASE))

            self.balance_at = time.time() + self.balance_interval

    def load_balance(self):
        """main loop"""
        while True:
            evts = self.poller.poll(500)
            if evts:
                msg = self.client_sock.recv()
                self.worker_sock.send(msg)
                self.balance_workers()
