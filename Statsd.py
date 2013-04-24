# statsd example

# Uses same config file as sheldon, be sure to provide a statsd section.
# Example:
# [statsd]
# host = localhost
# port = 8125
# prefix = sheldon.dev.
# suffix = 
 
import ConfigParser
import os

# Sends statistics to the stats daemon over UDP
global config
if "CONFIG_FILE" in os.environ.keys():
    # Expose uwsgi app
        config_file = os.environ["CONFIG_FILE"]
        config = ConfigParser.RawConfigParser()
        config.read(config_file)

class Statsd(object):
    
    @staticmethod
    def timing(stat, time, sample_rate=1):
        """
        Log timing information
        >>> from python_example import Statsd
        >>> Statsd.timing('some.time', 500)
        """
        stats = {}
        stats[stat] = "%d|ms" % time
        Statsd.send(stats, sample_rate)

    @staticmethod
    def increment(stats, sample_rate=1):
        """
        Increments one or more stats counters
        >>> Statsd.increment('some.int')
        >>> Statsd.increment('some.int',0.5)
        """
        Statsd.update_stats(stats, 1, sample_rate)

    @staticmethod
    def decrement(stats, sample_rate=1):
        """
        Decrements one or more stats counters
        >>> Statsd.decrement('some.int')
        """
        Statsd.update_stats(stats, -1, sample_rate)
    
    @staticmethod
    def update_stats(stats, delta=1, sampleRate=1):
        """
        Updates one or more stats counters by arbitrary amounts
        >>> Statsd.update_stats('some.int',10)
        """
        if (type(stats) is not list):
            stats = [stats]
        data = {}
        for stat in stats:
            data[stat] = "%s|c" % delta

        Statsd.send(data, sampleRate)
    
    @staticmethod
    def send(data, sample_rate=1):
        """
        Squirt the metrics over UDP
        """
        try:
            host = config.get("statsd", "host")
            port = int(config.get("statsd", "port"))
            prefix = config.get("statsd", "prefix")
            suffix = config.get("statsd", "suffix")
            addr=(host, port)
        except Error:
            exit(1)
        
        sampled_data = {}
        
        if(sample_rate < 1):
            import random
            if random.random() <= sample_rate:
                for stat in data.keys():
                    value = sampled_data[stat]
                    sampled_data[stat] = "%s|@%s" %(value, sample_rate)
        else:
            sampled_data=data
        
        from socket import *
        udp_sock = socket(AF_INET, SOCK_DGRAM)
        try:
            for stat in sampled_data.keys():
                value = data[stat]
                send_data = "%s%s%s:%s" % (prefix, stat, suffix, value)
                udp_sock.sendto(send_data, addr)
        except:
            import sys
            from pprint import pprint
            print "Unexpected error:", pprint(sys.exc_info())
            pass # we don't care
