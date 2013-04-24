import cloudfiles
import re
import threading

class ThreadGetWork(threading.Thread):
    """Populates work queue"""
    def __init__(self, queue, config, opt, state, l):
        """Call parent __init__ and set queue to read work from"""
        threading.Thread.__init__(self)
        self.queue = queue
        self.l = l
        self.opt = opt
        self.config = config
        self.state = state
        if opt.source:
            # source may not be set if we're deleting
            self.source = cloudfiles.get_connection(config.get(opt.source, "user"), config.get(opt.source, "password"), authurl=config.get(opt.source, "authurl"), timeout=30)
        else:
            self.source = None

        self.destination = cloudfiles.get_connection(config.get(opt.destination, "user"), config.get(opt.destination, "password"), authurl=config.get(opt.destination, "authurl"), timeout=30)
        self.quit = threading.Event()

    def get_cleaning_work(self, container_name):
        """Adds cleaning work to queue (all objects in specified swift container will be removed from the destination account)"""
        #self.l.info("Getting cleaning work for container %s" % container_name)
        swift_objects = self.destination.get_container(container_name).list_objects()
        if len(swift_objects) == 10000:
            self.l.debug("Got 10000 objects, getting more")
            more_stuff = True
        else:
            more_stuff = False
        while more_stuff:
            last_swift_object = swift_objects[len(swift_objects) - 1]
            more_swift_objects = self.destination.get_container(container_name).list_objects(marker=last_swift_object)
            if len(more_swift_objects) < 10000:
                more_stuff = False
            swift_objects = swift_objects + more_swift_objects
            more_swift_objects = []
        for obj in swift_objects:
            self.l.debug("added %s to queue" % obj)
            r = re.compile(self.opt.object_pattern)
            if r.match(obj):
                self.queue.put((container_name, obj))
        if len(swift_objects) == 0:
            self.l.debug("Deleting empty container %s" % container_name)
            self.destination.delete_container(container_name)

    def get_migration_work(self, container_name):
        """Adds work to queue. It connects to the source account and lists the specified container. If an objects pattern is specified, only selected objects will be added to the queue."""
        self.l.info("Getting migration work for container %s" % container_name)

        source_objects = self.source.get_container(container_name).list_objects_info()
        if len(source_objects) == 10000:
            self.l.debug("Found 10000 objects, getting more")
            more_stuff = True
        else:
            more_stuff = False
        while more_stuff:
            last_swift_object = source_objects[len(source_objects) - 1]["name"]
            more_source_objects = self.source.get_container(container_name).list_objects_info(marker=last_swift_object)
            self.l.debug("Found %s more objects from marker %s" % (len(more_source_objects), last_swift_object))

            if len(more_source_objects) < 10000:
                more_stuff = False
            source_objects = source_objects + more_source_objects
            more_source_objects = []

        self.l.info("Currently %s objects in source for container %s" % (len(source_objects), container_name))

        destination_objects = self.destination.get_container(container_name).list_objects_info()
        if len(destination_objects) == 10000:
            self.l.debug("Found 10000 objects, getting more")
            more_stuff = True
        else:
            more_stuff = False
        while more_stuff:
            last_swift_object = destination_objects[len(destination_objects) - 1]["name"]
            more_destination_objects = self.destination.get_container(container_name).list_objects_info(marker=last_swift_object)
            self.l.debug("Found %s more objects from marker %s" % (len(more_destination_objects), last_swift_object))
            if len(more_destination_objects) < 10000:
                more_stuff = False
            destination_objects = destination_objects + more_destination_objects
            more_destination_objects = []

        self.l.info("Currently %s objects in destination for container %s" % (len(destination_objects), container_name))

        destination_object_hashes = dict((x["name"], x["hash"]) for x in destination_objects)
        r = re.compile(self.opt.object_pattern)

        matched = 0
        for obj in source_objects:
            if obj["hash"] != destination_object_hashes.get(obj["name"]) and r.match(obj["name"]):
                matched += 1
                self.l.debug("Added %s to queue" % obj["name"])
                self.queue.put((container_name, obj["name"]))

        self.l.info("%s objects in container %s matched migration criteria" % (matched, container_name))

    def run(self):
        if self.source:
            source_container_list = self.source.list_containers_info()
            if len(source_container_list) == 10000:
                more_stuff = True
            else:
                more_stuff = False
            while more_stuff:
                last_swift_container = source_container_list[len(source_container_list) - 1]
                more_swift_containers = self.source.list_containers_info(marker=last_swift_container)
                if len(more_swift_containers) < 10000:
                    more_stuff = False
                source_container_list = source_container_list + more_swift_containers
                more_swift_containers = []

        # Get destination container list
        destination_container_list = self.destination.list_containers_info()
        if len(destination_container_list) == 10000:
            more_stuff = True
        else:
            more_stuff = False

        while more_stuff:
            last_swift_container = destination_container_list[len(destination_container_list) - 1]
            more_swift_containers = self.destination.list_containers_info(marker=last_swift_container)
            if len(more_swift_containers) < 10000:
                more_stuff = False
            destination_container_list = destination_container_list + more_swift_containers
            more_swift_containers = []

        r = re.compile(self.opt.container_pattern)
        if self.opt.delete:
            self.state["container_list"] = [x["name"] for x in destination_container_list if r.match(x["name"])]
            self.l.info("%s containers in destination account, %s matched by container_pattern" % (len(destination_container_list), len(self.state["container_list"])))

        else:
            self.state["container_list"] = [x["name"] for x in source_container_list if r.match(x["name"])]
            self.l.info("%s containers in source account, %s matched by container_pattern" % (len(source_container_list), len(self.state["container_list"])))

        i = 0

        while not self.quit.isSet():
            i += 1
            if self.queue.qsize() < 2000:
                #self.l.info("Queue size under 2000 - need more work")
                if len(self.state["container_list"]) > 0:
                    container = self.state["container_list"].pop(0)
                else:
                    self.l.info("No containers available")
                    self.quit.set()
                    break

                #try:
                if self.opt.delete:
                    self.get_cleaning_work(container)
                else:
                    self.destination.create_container(container)
                    self.get_migration_work(container)
                #except:
                #    # Re-append this number to the list
                #    self.quit.set()
                #    self.l.error("Something went wrong while getting work for number %s" % number)
                #    self.state["number"].append(number)
        self.l.info("get-work thread terminating")

    def stop(self):
        self.quit.set()
