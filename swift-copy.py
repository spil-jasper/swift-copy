#!/usr/bin/env python
# Author: Jasper Capel
# This program migrates objects between swift accounts

import argparse
import ConfigParser
import datetime
import os
import Queue
#import resource
import signal
import sys
import threading
import time

from ThreadCleaner import *
from ThreadGetWork import *
from ThreadMigrator import *
#resource.setrlimit(resource.RLIMIT_NOFILE, (20480, 20480))


class Logger():
    """Helper class to log messages"""
    DEBUG = 2
    ERROR = 0
    INFO = 1
    level = 2
    levels = {0: "ERROR", 1: "INFO", 2: "DEBUG"}
    fp = None

    def __init__(self, level, file=None):
        self.level = level
        if file:
            self.fp = open(file, "a")

    def l(self, level, line):
        line = "(%s) %s" % (self.levels[level], line)
        if self.level >= level:
            print line
        if self.fp:
            now = datetime.datetime.now()
            self.fp.write("%s -- %s\n" % (now, line))

    def debug(self, line):
        self.l(self.DEBUG, line)

    def error(self, line):
        self.l(self.ERROR, line)

    def info(self, line):
        self.l(self.INFO, line)


class Migrator():
    """Main program class"""
    stop_threads = False
    threads = []
    queue = None
    l = None
    quitting = False
    quit = False
    state = {"queue": [], "failed": [], "404": [], "done": 0, "container_list": [], "bytes": 0}
    opt = None
    config = None

    def __init__(self):
        """Program entry point"""
        op = argparse.ArgumentParser()

        op.add_argument("-c", "--config", dest="file", default="swift-copy.conf", help="Configuration file")
        op.add_argument("-d", "--debug", dest="debug", help="Debug mode", default=False, action="store_true")
        op.add_argument("-D", "--delete", dest="delete", help="Run in delete mode (empty target swift account)", action="store_true")

        op.add_argument("-1", "--source", dest="source", help="source environment (defined in config)", metavar="ENV")
        op.add_argument("-2", "--destination", dest="destination", help="destination environment (defined in config)", metavar="ENV")

        op.add_argument("-l", "--log", dest="log", help="Log to file", metavar="FILE")
        op.add_argument("-C", "--container", dest="container_pattern", default=".*", help="if set, only containers matching this pattern will be migrated", metavar="PATTERN")
        op.add_argument("-O", "--object-pattern", dest="object_pattern", default=".*", help="if set, only objects matching pattern will be migrated", metavar="PATTERN")
        op.add_argument("-r", "--resume", dest="resume", help="Resume from state file", action="store_true", default=False)
        op.add_argument("-q", "--quiet", dest="quiet", help="Only show errors", default=False, action="store_true")
        op.add_argument("-s", "--state", dest="state", help="State file", metavar="FILE")
        op.add_argument("-t", "--threads", dest="threads", default=1, type=int, help="Number of threads to spawn")

        self.opt = op.parse_args()
        opt = self.opt

        if opt.quiet and opt.debug:
            print "Quiet and debug are mutually exclusive"
            sys.exit(1)

        if opt.debug:
            l = Logger(Logger.DEBUG, opt.log)
        elif opt.quiet:
            l = Logger(Logger.ERROR, opt.log)
        else:
            l = Logger(Logger.INFO, opt.log)

        # Make logger accessible from the entire class
        self.l = l

        # Set up queue
        self.queue = Queue.Queue()

        if opt.resume:
            if not opt.state:
                l.error("No state file specified")
                sys.exit(1)
            l.info("Resuming from state file")
            fp = open(opt.state)
            self.resume(pickle.load(fp))
            opt = self.opt
            config = self.config
            fp.close()
        else:
            if (not opt.source or not opt.destination) and not opt.delete:
                l.error("Source and destination are mandatory when migrating")
                op.print_help()
                sys.exit(1)
            if opt.delete and not opt.destination:
                l.error("Destination is mandatory when deleteing")
                sys.exit(1)
            if opt.delete and opt.source:
                l.error("Source does not make sense when deleting")
                sys.exit(1)
            config = ConfigParser.ConfigParser()
            if not config.read(opt.file):
                l.error("Error reading/parsing %s" % opt.file)
                sys.exit(1)
            self.config = config
            if opt.state and os.path.exists(opt.state) and not opt.force:
                print "State file already exists, use -f to overwrite or -r to resume operation"
                sys.exit(1)

        # Register signal handler
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        # Spawn get-work thread
        t = ThreadGetWork(self.queue, self.config, self.opt, self.state, l)
        t.start()
        self.threads.append(t)

        start = time.time()

        # Spawn threads pass config, options, logger and thread number into thread
        for i in range(opt.threads):
            if self.quit:
                # Possible race condition: killed before we're done spawning all
                # threads
                break
            l.debug("Spawning thread %s" % i)
            if opt.delete:
                t = ThreadCleaner(self.queue, self.config, self.opt, self.state, l, i)
            else:
                t = ThreadMigrator(self.queue, self.config, self.opt, self.state, l, i)
            t.setDaemon(True)
            t.start()
            self.threads.append(t)
        l.info("%s threads started, entering main loop" % self.opt.threads)

        # Display progress
        workdone = self.state["done"]
        bytes = self.state["bytes"]
        lastreport = time.time()
        while not self.quit:

            time.sleep(1)
            if (time.time() - lastreport) >= 60:
                if self.queue.qsize() == 0:
                    if len(self.state["container_list"]) == 0:
                        self.l.info("Queue empty, backlog fully processed")
                        break
                    else:
                        self.l.info("Queue empty, but backlog not empty, waiting.")
                oldworkdone = workdone
                oldbytes = bytes
                workdone = self.state["done"]
                bytes = self.state["bytes"]
                delta = workdone - oldworkdone
                work_per_second = (delta / (time.time() - lastreport))
                delta = bytes - oldbytes
                kbps = (delta / (time.time() - lastreport))
                lastreport = time.time()
                l.info("Queue: %s obj (%sobj/s, %sKB/s) | Backlog: %s cont" % (self.queue.qsize(), round(work_per_second, 1), round(kbps / 1024, 1), len(self.state["container_list"])))
        finish = time.time()
        elapsed = finish - start
        avg_speed_o = self.state["done"] / elapsed
        avg_speed_b = self.state["bytes"] / elapsed / 1024
        l.info("Time elapsed: %s seconds, objects processed: %s, average speed %sobj/s %sKB/s " % (elapsed, self.state["done"], round(avg_speed_o), round(avg_speed_b)))
        if self.quit:
            # We got here by a quit signal, not by queue depletion
            sys.exit(0)
        l.info("Waiting for threads")
        self.queue.join()
        self.save()

        l.debug("Queue empty, exiting")

    def resume(self, state):
        """Resume state"""
        self.state = state
        for id in self.state["queue"]:
            self.queue.put(id)
        self.state["queue"] = []
        self.opt = self.state["opt"]
        self.config = self.state["config"]

    def save(self):
        """Save state to state file"""
        if self.opt.state:
            while self.queue.qsize() > 0:
                self.state["queue"].append(self.queue.get())
            self.state["opt"] = self.opt
            self.state["config"] = self.config
            fp = open(self.opt.state, "w+")
            pickle.dump(self.state, fp)
            fp.close()
            self.l.debug("Wrote state to %s" % self.opt.state)

    def signal_handler(self, signal, frame):
        self.l.error("Caught CTRL+C / SIGKILL")
        if not self.quitting:
            self.quitting = True
            self.stop_threads()
            self.save()
            self.quit = True
        else:
            self.l.error("BE PATIENT!@#~!#!@#$~!`1111")

    def stop_threads(self):
        """Stops all threads and waits for them to quit"""
        self.l.info("Stopping threads")
        for thread in self.threads:
            thread.stop()
        while threading.activeCount() > 1:
            self.l.debug("Waiting for %s threads" % threading.activeCount())
            time.sleep(1)
        self.l.info("All threads stopped")


program = Migrator()
