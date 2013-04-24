import cloudfiles
import threading
import time
import Statsd


class ThreadMigrator(threading.Thread):
    """Threaded worker to fetch data from source and upload to destination"""
    def __init__(self, queue, config, opt, state, l, i):
        """Call parent __init__ and set queue to read work from"""
        threading.Thread.__init__(self)
        self.queue = queue
        self.l = l
        self.i = i
        self.opt = opt
        self.config = config
        Statsd.config = config
        self.state = state
        self.source = cloudfiles.get_connection(config.get(opt.source, "user"), config.get(opt.source, "password"), authurl=config.get(opt.source, "authurl"), timeout=30)
        self.destination = cloudfiles.get_connection(config.get(opt.destination, "user"), config.get(opt.destination, "password"), authurl=config.get(opt.destination, "authurl"), timeout=30)
        self.quit = threading.Event()

    def run(self):
        """Run thread until we run out of work"""

        self.l.debug("Hi, I'm a worker thread")
        current_container = None

        while not self.quit.isSet():
            # Grab work from queue
            try:
                (container_name, object_name) = self.queue.get(timeout=60)
                self.l.debug("[%s] took %s from queue" % (self.i, object_name))
            except:
                self.l.info("[%s] No work for 60 seconds" % self.i)
                # Continue next loop iteration, maybe the quit flag has been set
                continue

            # Switch to new container if new item is in another container
            if current_container != container_name:
                s_container = self.source.get_container(container_name)
                d_container = self.destination.get_container(container_name)
                current_container = container_name

            # Stream object from source to destination

            try:
                s_obj = s_container.get_object(object_name)
                d_obj = d_container.create_object(object_name)
                d_obj.content_type = s_obj.content_type
                start = time.time()
                d_obj.send(s_obj.stream())
                delta = time.time() - start
                Statsd.Statsd.timing("dev.syseng.swift-copy.%s.GET_stream" % self.opt.source, delta * 1000)
                Statsd.Statsd.timing("dev.syseng.swift-copy.%s.PUT_stream" % self.opt.destination, delta * 1000)
                size = s_obj.size
                Statsd.Statsd.increment("dev.syseng.swift-copy.%s.GET" % self.opt.source, size)
                Statsd.Statsd.increment("dev.syseng.swift-copy.%s.PUT" % self.opt.destination, size)
                Statsd.Statsd.update_stats("dev.syseng.swift-copy.%s.bytes_r" % self.opt.source, size)
                Statsd.Statsd.update_stats("dev.syseng.swift-copy.%s.bytes_w" % self.opt.destination, size)
                with threading.Lock():
                    self.state["bytes"] += size
                    self.state["done"] += 1
            except cloudfiles.errors.NoSuchObject as e:
                self.l.debug("Could not find %s on source" % object_name)
                Statsd.Statsd.increment("dev.syseng.swift-copy.%s.GET_404" % self.opt.source)
                # Don't requeue failing object and continue
                self.queue.task_done()
                continue
            except Exception as e:
                self.l.debug("%s exception while streaming %s (requeueing)" % (e.message, object_name))
                Statsd.Statsd.increment("dev.syseng.swift-copy.%s.GET_stream_ERR" % self.opt.source)
                Statsd.Statsd.increment("dev.syseng.swift-copy.%s.PUT_stream_ERR" % self.opt.destination)

                # requeue object
                self.queue.put((container_name, object_name))
                self.queue.task_done()
                # continue with next iteration
                continue

            # try:
            #     start = time.time()
            #     data = s_obj.read()
            #     delta = time.time() - start
            #     Statsd.Statsd.timing("dev.syseng.swift-copy.%s.GET" % self.opt.source, delta * 1000)
            #     size = s_obj.size
            #     Statsd.Statsd.update_stats("dev.syseng.swift-copy.%s.bytes_r" % self.opt.source, size)

            # except Exception as e:
            #     self.l.debug("%s exception while reading %s from source (requeueing)" % (e.message, object_name))
            #     Statsd.Statsd.increment("dev.syseng.swift-copy.%s.GET_ERR" % self.opt.source)
            #     # requeue object
            #     self.queue.put((container_name, object_name))
            #     self.queue.task_done()
            #     # continue with next iteration
            #     continue

            # try:
            #     start = time.time()
            #     d_obj.send(data)
            #     delta = time.time() - start
            #     Statsd.Statsd.timing("dev.syseng.swift-copy.%s.PUT" % self.opt.destination, delta * 1000)

            #     Statsd.Statsd.update_stats("dev.syseng.swift-copy.%s.bytes_w" % self.opt.destination, size)
            #     self.l.debug("[%s] %s/%s (%s bytes) done" % (self.i, container_name, object_name, size))
            #     with threading.Lock():
            #         self.state["bytes"] += size
            #         self.state["done"] += 1

            # except Exception as e:
            #     self.l.debug("%s exception while writing %s to target (requeueing)" % (e.message, object_name))
            #     self.queue.put((container_name, object_name))
            #     Statsd.Statsd.increment("dev.syseng.swift-copy.%s.PUT_ERR" % self.opt.destination)

            # Signal done
            self.queue.task_done()

        self.l.debug("[%s] Stopped" % self.i)

    def stop(self):
        self.quit.set()
