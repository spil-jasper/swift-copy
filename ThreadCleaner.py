import cloudfiles
import threading


class ThreadCleaner(threading.Thread):
    """Threaded container cleaner"""
    def __init__(self, queue, config, opt, state, l, i):
        """Call parent __init__ and set queue to read work from"""
        threading.Thread.__init__(self)
        self.queue = queue
        self.l = l
        self.i = i
        self.opt = opt
        self.config = config
        self.state = state
        self.swift = cloudfiles.get_connection(config.get(opt.destination, "user"), config.get(opt.destination, "password"),
            authurl=config.get(opt.destination, "authurl"), timeout=30)
        self.quit = threading.Event()

    def run(self):
        """Run thread until we run out of work"""
        current_container = None
        container = None
        while not self.quit.isSet():
            # Grab work from queue
            (container_name, obj) = self.queue.get()
            self.l.debug("[%s] took %s from queue" % (self.i, obj))
            if current_container != container_name:
                if container and len(container.list_objects()) == 0:
                    self.l.debug("[%s] deleting %s since it's empty" % (self.i, current_container))
                    self.swift.delete_container(current_container)
                container = self.swift.get_container(container_name)
                current_container = container_name
            try:
                container.delete_object(obj)
            except:
                self.l.error("[%s] deleting %s failed" % (self.i, obj))
                continue
            self.l.debug("[%s] deleted %s OK" % (self.i, obj))
            self.queue.task_done()
            with threading.Lock():
                self.state["done"] += 1
        self.l.debug("[%s] Stopped" % self.i)

    def stop(self):
        self.quit.set()
