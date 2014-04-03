#!/usr/bin/python26

"Parses metric files and show destination backend for every metric path"

usage = "%prog [OPTIONS] <path/to/relay.conf> <path/to/logs|metric name>"

epilog = """If neither --is-file nor --is-metric-path are provided second argument type will be a guess"""

import sys
import imp
from os.path import dirname, join, abspath, exists, realpath, splitext
from os import listdir
from optparse import OptionParser

# Figure out where we're installed
BIN_DIR = dirname(abspath(__file__))
ROOT_DIR = dirname(BIN_DIR)
CONF_DIR = join(ROOT_DIR, 'conf')
default_relayrules = join(CONF_DIR, 'relay.conf')

# Make sure that carbon's 'lib' dir is in the $PYTHONPATH if we're running from
# source.
LIB_DIR = join(ROOT_DIR, 'lib')
sys.path.insert(0, LIB_DIR)

from carbon.routers import ConsistentHashingRouter
from carbon import util

def setupOptionParser():
    parser = OptionParser(usage=usage, epilog=epilog)
    parser.add_option('--is-file',
                default=False,
                action = 'store_true',
                help='Interpret second argument as path to directory with files')
    parser.add_option('--is-metric-path',
                default=False,
                action = 'store_true',
                help='Interpret second argument as metric path')
    parser.add_option('--show-port',
                default=False,
                action = 'store_true',
                help='Show backend port number and name')
    parser.add_option('--ignore-keyfunc',
                default=False,
                action = 'store_true',
                help='Do not load keyfunc even if it is present in the carbon config file')
    return parser

def parse_config(config_path):
    options = {}
    f = open(config_path)
    for l in f.readlines():
        l = l.strip()
        if len(l) == 0 or l.startswith('#'):
            continue
        k, v = l.split('=')
        k = k.strip()
        v = v.strip()
        options[k] = v
    if options.has_key('DESTINATIONS'):
        destinations_strings = [v.strip() for v in options['DESTINATIONS'].split(',')]
        options['DESTINATIONS'] = util.parseDestinations(destinations_strings)
    return options

def configure_router(config):
    if config['RELAY_METHOD'] == 'consistent-hashing':
        router = ConsistentHashingRouter(int(config['REPLICATION_FACTOR']))
    else:
        print "Unsupported relay method '%s'" % config['RELAY_METHOD']
        sys.exit(1)
    return router

def list_logs(log_file_path):
    for l in listdir(log_file_path):
        if splitext(l)[-1] == '.log':
            yield abspath(join(log_file_path, l))

def list_metric_lines(log_file):
    f = open(log_file)
    for l in f.readlines():
        yield(l)

def get_metrics(log_file_path):
    for log_file in list_logs(log_file_path):
        for metric_line in list_metric_lines(log_file):
            yield metric_line.split()[0]

def autodetect_path_arg(metric_path):
    if len(metric_path.split('/')) > 3:
        return True
    if len(metric_path.split('.')) > 3:
        return False
    return None

def print_destinations(router, metric, show_port=False):
    for d in router.getDestinations(metric):
        # do not show stacktrace for a broken pipe, e.g. if piped into |head
        try:
            if show_port:
                print '%s  ->  %s:%d:%s' % (metric, d[0], d[1], d[2])
            else:
                print '%s  ->  %s' % (metric, d[0])
        except IOError:
            sys.exit(2)

def main():
    opts_parser = setupOptionParser()
    (options, args) = opts_parser.parse_args()
    if len(args) < 2:
        opts_parser.error("This tool takes at least two arguments")
        sys.exit(1)
    carbon_config_path = args[0]
    metric_path = args[1]
    is_file = None
    if options.is_file and options.is_metric_path:
        print "Error: --is-file and --is-metric-path options are mutually exclusive."
        sys.exit(1)
    elif options.is_file:
        is_file = True
    elif options.is_metric_path:
        is_file = False
    else:
        is_file = autodetect_path_arg(metric_path)
    if is_file is None:
        print """Can not recognize if %s is log directory or graphite metric path.
Please provide either --is-file or --is-metric-path option"""
        sys.exit(1)
    config = parse_config(carbon_config_path)
    router = configure_router(config)
    if not options.ignore_keyfunc and config.has_key('KEYFUNC'):
        router.setKeyFunctionFromModule(config['KEYFUNC'])
    for d in config['DESTINATIONS']:
        router.addDestination(d)
    if is_file:
        for m in get_metrics(metric_path):
            print_destinations(router, m, options.show_port)
    else:
        print_destinations(router, metric_path, options.show_port)

if __name__ == '__main__':
    main()

