import re
import time
import logging
from math import floor
from logging.handlers import TimedRotatingFileHandler
import datetime
from influxdb import InfluxDBClusterClient
try:
    import statsd
except ImportError:
    pass

logger = logging.getLogger('graphite_influxdb')

try:
    from graphite_api.intervals import Interval, IntervalSet
    from graphite_api.node import LeafNode, BranchNode
except ImportError:
    try:
        from graphite.intervals import Interval, IntervalSet
        from graphite.node import LeafNode, BranchNode
    except ImportError:
        raise SystemExit(1, "You have neither graphite_api nor \
    the graphite webapp in your pythonpath")

# Tell influxdb to return time as seconds from epoch
_INFLUXDB_CLIENT_PARAMS = {'epoch' : 's'}

class NullStatsd():
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def timer(self, key, val=None):
        return self

    def timing(self, key, val):
        pass

    def start(self):
        pass

    def stop(self):
        pass


def normalize_config(config=None):
    ret = {}
    if config is not None:
        cfg = config.get('influxdb', {})
        ret['host'] = cfg.get('host', 'localhost')
        ret['user'] = cfg.get('user', 'graphite')
        ret['passw'] = cfg.get('pass', 'graphite')
        ret['db'] = cfg.get('db', 'graphite')
        ssl = cfg.get('ssl', False)
        ret['ssl'] = (ssl == 'true')
        ret['schema'] = cfg.get('schema', [])
        ret['min_data_points'] = cfg.get('min_data_points', 0)
        ret['log_file'] = cfg.get('log_file', None)
        ret['log_level'] = cfg.get('log_level', 'info')
        cfg = config.get('es', {})
        ret['es_enabled'] = cfg.get('enabled', False)
        ret['es_index'] = cfg.get('index', 'graphite_metrics2')
        ret['es_hosts'] = cfg.get('hosts', ['localhost:9200'])
        ret['es_field'] = cfg.get('field', '_id')
        if config.get('statsd', None):
            ret['statsd'] = config.get('statsd')
    else:
        from django.conf import settings
        ret['host'] = getattr(settings, 'INFLUXDB_HOST', 'localhost')
        ret['user'] = getattr(settings, 'INFLUXDB_USER', 'graphite')
        ret['passw'] = getattr(settings, 'INFLUXDB_PASS', 'graphite')
        ret['db'] = getattr(settings, 'INFLUXDB_DB', 'graphite')
        ssl = getattr(settings, 'INFLUXDB_SSL', False)
        ret['ssl'] = (ssl == 'true')
        ret['schema'] = getattr(settings, 'INFLUXDB_SCHEMA', [])
        ret['min_data_points'] = getattr(settings, 'INFLUXDB_MIN_DATA_POINTS', 0)
        ret['log_file'] = getattr(
            settings, 'INFLUXDB_LOG_FILE', None)
        # Default log level is 'info'
        ret['log_level'] = getattr(
            settings, 'INFLUXDB_LOG_LEVEL', 'info')
        ret['es_enabled'] = getattr(settings, 'ES_ENABLED', False)
        ret['es_index'] = getattr(settings, 'ES_INDEX', 'graphite_metrics2')
        ret['es_hosts'] = getattr(settings, 'ES_HOSTS', ['localhost:9200'])
        ret['es_field'] = getattr(settings, 'ES_FIELD', '_id')
    return ret

def _make_graphite_api_points_list(influxdb_data):
    """Make graphite-api data points dictionary from Influxdb ResultSet data"""
    _data = {}
    for key in influxdb_data.keys():
        _data[key[0]] = [(datetime.datetime.fromtimestamp(float(d['time'])),
                          d['value']) for d in influxdb_data.get_points(key[0])]
    return _data

class InfluxdbReader(object):
    __slots__ = ('client', 'path', 'step', 'statsd_client')

    def __init__(self, client, path, step, statsd_client):
        self.client = client
        self.path = path
        self.step = step
        self.statsd_client = statsd_client

    def fetch(self, start_time, end_time):
        # in graphite,
        # from is exclusive (from=foo returns data at ts=foo+1 and higher)
        # until is inclusive (until=bar returns data at ts=bar and lower)
        # influx doesn't support <= and >= yet, hence the add.
        logger.debug("fetch() path=%s start_time=%s, end_time=%s, step=%d", self.path, start_time, end_time, self.step)
        with self.statsd_client.timer('service_is_graphite-api.ext_service_is_influxdb.target_type_is_gauge.unit_is_ms.what_is_query_individual_duration'):
            _query = 'select mean(value) as value from "%s" where (time > %ds and time <= %ds) GROUP BY time(%ss)' % (
                self.path, start_time, end_time, self.step)
            logger.debug("fetch() path=%s querying influxdb query: '%s'", self.path, _query)
            data = self.client.query(_query, params=_INFLUXDB_CLIENT_PARAMS)
        logger.debug("fetch() path=%s returned data: %s", self.path, data)
        try:
            data = _make_graphite_api_points_list(data)
        except Exception:
            logger.debug("fetch() path=%s COULDN'T READ POINTS. SETTING TO EMPTY LIST", self.path)
            data = []
        time_info = start_time, end_time, self.step
        return time_info, [v[1] for v in data[self.path]]

    def get_intervals(self):
        now = int(time.time())
        return IntervalSet([Interval(1, now)])


class InfluxLeafNode(LeafNode):
    __fetch_multi__ = 'influxdb'


class InfluxdbFinder(object):
    __fetch_multi__ = 'influxdb'
    __slots__ = ('client', 'es', 'schemas', 'config', 'statsd_client')

    def __init__(self, config=None):
        # Shouldn't be trying imports in __init__.
        # It turns what should be a load error into a runtime error
        config = normalize_config(config)
        self.config = config
        self.client = InfluxDBClusterClient(config['host'], config['user'], config['passw'], config['db'], config['ssl'])
        self.schemas = [(re.compile(patt), step) for (patt, step) in config['schema']]
        try:
            self.statsd_client = statsd.StatsClient(config['statsd'].get('host'),
                                                    config['statsd'].get('port', 8125)) \
                                                    if 'statsd' in config and config['statsd'].get('host') else NullStatsd()
        except NameError:
            logger.warning("Statsd client configuration present but 'statsd' module"
                           "not installed - ignoring statsd configuration..")
            self.statsd_client = NullStatsd()
        self._setup_logger(config['log_level'], config['log_file'])
        self.es = None
        if config['es_enabled']:
            try:
                from elasticsearch import Elasticsearch
            except ImportError:
                logger.warning("Elasticsearch configuration present but 'elasticsearch'"
                               "module not installed - ignoring elasticsearch configuration..")
            else:
                self.es = Elasticsearch(config['es_hosts'])

    def _setup_logger(self, level, log_file):
        """Setup log level and log file if set"""
        if logger.handlers:
            return
        level = getattr(logging, level.upper())
        logger.setLevel(level)
        formatter = logging.Formatter(
            '[%(levelname)s] %(asctime)s - %(module)s.%(funcName)s() - %(message)s')
        handler = logging.StreamHandler()
        logger.addHandler(handler)
        handler.setFormatter(formatter)
        if not log_file:
            return
        try:
            handler = TimedRotatingFileHandler(log_file)
        except IOError:
            logger.error("Could not write to %s, falling back to stdout",
                         log_file)
        else:
            logger.addHandler(handler)
            handler.setFormatter(formatter)

    # This method SHOULD NOT be used anywhere anymore. It is superseeded
    # by find_leaves and find_branches.
    # We leave it here for now for the ES logic. It is not decided yet if
    # we want to port this or discard it, but effectively for the moment
    # there is no ES support
    def assure_series(self, query):
        key_series = "%s_series" % query.pattern
        logger.error("assure_series() THIS METHOD SHOULD NOT BE CALLED - %s", key_series)
        return
        done = False
        if self.es:
            # note: ES always treats a regex as anchored at start and end
            regex = self.compile_regex('{0}.*', query)
            with self.statsd_client.timer('service_is_graphite-api.ext_service_is_elasticsearch.target_type_is_gauge.unit_is_ms.action_is_get_series'):
                logger.debug("assure_series() Calling ES with regexp - %s", regex.pattern)
                try:
                    res = self.es.search(index=self.config['es_index'],
                                         size=10000,
                                         body={
                                             "query": {
                                                 "regexp": {
                                                     self.config['es_field']: regex.pattern,
                                                 },
                                             },
                                             "fields": [self.config['es_field']]
                                         }
                                         )
                    if res['_shards']['successful'] > 0:
                        # pprint(res['hits']['total'])
                        series = [hit['fields'][self.config['es_field']] for hit in res['hits']['hits']]
                        done = True
                    else:
                        logger.error("assure_series() Calling ES failed for %s: no successful shards", regex.pattern)
                except Exception as e:
                    logger.error("assure_series() Calling ES failed for %s: %s", regex.pattern, e)
        # if no ES configured, or ES failed, try influxdb.
        if not done:
            # regexes in influxdb are not assumed to be anchored, so anchor them explicitly
            regex = self.compile_regex('^{0}', query)
            logger.debug("assure_series() Calling influxdb with query - %s", query.pattern)
            path=query.pattern.split('.')
            logger.debug("assure_series() path - %s", path)
            with self.statsd_client.timer('service_is_graphite-api.ext_service_is_influxdb.target_type_is_gauge.unit_is_ms.action_is_get_series'):
                _query = "show series where t0 =~ /%s/" % self.my_compile_regex('^{0}$', path[0]).pattern
                i=1
                while i < len(path):
                    _query += " AND t%i =~ /%s/" % (i, self.my_compile_regex('^{0}$', path[i]).pattern)
                    i += 1
                ret = self.client.query(_query, params=_INFLUXDB_CLIENT_PARAMS)
                series = [self.my_convert_to_path(key_name['_key']) for key_name in ret.get_points()]

                _query = "show series from /%s/" % self.my_compile_regex('^{0}$', path.pop()).pattern
                if len(path) > 0:
                    _query += " WHERE t0 =~ /%s/" % self.my_compile_regex('^{0}$', path[0]).pattern
                    i=1
                    while i < len(path):
                        _query += " AND t%i =~ /%s/" % (i, self.my_compile_regex('^{0}$', path[i]).pattern)
                        i += 1
                logger.debug("assure_series() Calling influxdb with query - %s", _query)
                ret = self.client.query(_query, params=_INFLUXDB_CLIENT_PARAMS)
                series.extend([self.my_convert_to_path(key_name['_key']) for key_name in ret.get_points()])
        return series

    def my_convert_to_path(self, series_key):
        series_key = re.split(r'(?<!\\),', series_key)
        path = []
        regex = re.compile('^t[0-9]+=.+')
        for tag in series_key[1:]:
            #ignore tags that don't fit the pattern (like tn, version, region, etc).
            if regex.match(tag):
                tag_key, tag_value = tag.split('=')
                path.append(tag_value)

        path.append(series_key[0])
        return '.'.join(path)

    def my_compile_regex(self, fmt, query):
        """Turn glob (graphite) queries into compiled regex
        * becomes .*
        . becomes \.
        fmt argument is so that caller can control anchoring (must contain exactly 1 {0} !"""
        return re.compile(fmt.format(
            query.replace('.', '\.').replace('*', '[^\.]*').replace(
                '{', '(').replace(',', '|').replace('}', ')')
        ))

    def compile_regex(self, fmt, query):
        """Turn glob (graphite) queries into compiled regex
        * becomes .*
        . becomes \.
        fmt argument is so that caller can control anchoring (must contain exactly 1 {0} !"""
        return re.compile(fmt.format(
            query.pattern.replace('.', '\.').replace('*', '[^\.]*').replace(
                '{', '(').replace(',', '|').replace('}', ')')
        ))

    def get_leaves(self, query):
        key_leaves = "%s_leaves" % query.pattern
        logger.debug("get_leaves() key %s", key_leaves)
        series = self.find_leaves(query)
        regex = self.compile_regex('^{0}$', query)
        logger.debug("get_leaves() regex %s", regex.pattern)
        timer = self.statsd_client.timer('service_is_graphite-api.action_is_find_leaves.target_type_is_gauge.unit_is_ms')
        now = datetime.datetime.now()
        timer.start()
        # return every matching series and its
        # resolution (based on first pattern match in schema, fallback to 60s)
        leaves = [
                    (
                        name, next(
                            (res
                                for (patt, res) in self.schemas if patt.match(name)
                            ), 60
                        )
                    )
                  for name in series if regex.match(name)
                  ]
        timer.stop()
        end = datetime.datetime.now()
        dt = end - now
        logger.debug("get_leaves() key %s Finished find_leaves in %s.%ss",
                     key_leaves,
                     dt.seconds,
                     dt.microseconds)
        logger.debug("get_leaves() result %s", leaves)
        return leaves

    def find_leaves(self, query):
        key_series = "%s_series" % query.pattern
        # regexes in influxdb are not assumed to be anchored, so anchor them explicitly
        regex = self.compile_regex('^{0}', query)
        logger.debug("find_leaves() Calling influxdb with pattern - %s", query.pattern)
        path=query.pattern.split('.')
        path_length = len(path)
        logger.debug("find_leaves() path - %s (len: %s)", path, path_length)
        with self.statsd_client.timer('service_is_graphite-api.ext_service_is_influxdb.target_type_is_gauge.unit_is_ms.action_is_get_series'):
            _query = "show series from /%s/" % self.my_compile_regex('^{0}$', path.pop()).pattern
            if len(path) > 0:
                _query += " WHERE t0 =~ /%s/" % self.my_compile_regex('^{0}$', path[0]).pattern
                i=1
                while i < len(path):
                    _query += " AND t%i =~ /%s/" % (i, self.my_compile_regex('^{0}$', path[i]).pattern)
                    i += 1
                _query += " AND tn = '%i'" %( i-1)
            else:
                _query += " WHERE tn = '-1'"
            logger.debug("find_leaves() Calling influxdb with query - %s", _query)
            ret = self.client.query(_query, params=_INFLUXDB_CLIENT_PARAMS)
            series = [self.my_convert_to_path(key_name['_key']) for key_name in ret.get_points()]
            logger.debug("find_leaves() %i results", len(series))
        return series

    def get_branches (self, query):
        key_series = "%s_series" % query.pattern
        # regexes in influxdb are not assumed to be anchored, so anchor them explicitly
        regex = self.compile_regex('^{0}', query)
        logger.debug("find_branches() Calling influxdb with pattern - %s", query.pattern)
        path=query.pattern.split('.')
        logger.debug("find_branches() path - %s", path)
        key = "t%s" % (len(path)-1)
        with self.statsd_client.timer('service_is_graphite-api.ext_service_is_influxdb.target_type_is_gauge.unit_is_ms.action_is_get_series'):
            #_query = "show series where t0 =~ /%s/" % self.my_compile_regex('^{0}$', path[0]).pattern
            _query = "show tag values with key = %s" % key
            _query += " where t0 =~ /%s/"% self.my_compile_regex('^{0}$', path[0]).pattern
            i=1
            while i < len(path):
                tag_value_pattern = self.my_compile_regex('^{0}$', path[i]).pattern
                # Avoid matching on ^.*$. This is just wasted computing time, and actually makes queries a lot slower.
                # Some Numbers:
                # 1) show tag values with key = t3 where t0 =~ /^collectd$/ AND t1 =~ /^staging$/ AND t2 =~ /^cassandra-1$/ AND t3 =~ /^u[^\.]*$/
                #    Takes 2-3s at first with cold cache, than ~0.7 with warm cache (~10 runs)
                # 2) show tag values with key = t3 where t0 =~ /^collectd$/ AND t1 =~ /^staging$/ AND t2 =~ /^cassandra-1$/
                #    takes ~2s with cold cache, than ~0.6 with warm cache (also ~10 runs
                # 3) show tag values with key = t3 where t0 =~ /^collectd$/ AND t1 =~ /^staging$/ AND t2 =~ /^cassandra-1$/ AND t3 =~ /^[^\.]*$/'
                #    takes consistently 17-19s over 10 tests (to big to cache?)
                if tag_value_pattern != '^[^\.]*$':
                    _query += " AND t%i =~ /%s/" % (i, tag_value_pattern)
                i += 1
            ret = self.client.query(_query, params=_INFLUXDB_CLIENT_PARAMS)
            branches = [key_name[key] for key_name in ret.get_points()]
        logger.debug("find_branches() result - %s", branches)
        return branches

    def find_nodes(self, query):
        logger.debug("find_nodes() query %s", query)
        # TODO: once we can query influx better for retention periods, honor the start/end time in the FindQuery object
        with self.statsd_client.timer('service_is_graphite-api.action_is_yield_nodes.target_type_is_gauge.unit_is_ms.what_is_query_duration'):
            for (name, res) in self.get_leaves(query):
                yield InfluxLeafNode(name, InfluxdbReader(
                    self.client, name, res, self.statsd_client))
            for name in self.get_branches(query):
                logger.debug("Yielding branch %s" % (name,))
                yield BranchNode(name)

    def create_fetch_query(self, path, start_time, end_time, step):
        path = path.split('.')
        _query = "select mean(value) as value from /%s/ WHERE" % self.my_compile_regex('^{0}$', path.pop()).pattern
        if len(path) > 0:
            _query += " t0 =~ /%s/ AND" % self.my_compile_regex('^{0}$', path[0]).pattern
            i=1
            while i < len(path):
                _query += " t%i =~ /%s/ AND" % (i, self.my_compile_regex('^{0}$', path[i]).pattern)
                i += 1
        _query += " (time > %ds and time <= %ds) GROUP BY time(%ss)" % (start_time, end_time, step)
        return _query

    def _my_make_graphite_api_points_list(self, influxdb_data, nodes):
        """Make graphite-api data points dictionary from Influxdb ResultSet data"""
        _data = {}
        #logger.debug('_my_make_graphite_api_points_list() influxdb_data: %s', influxdb_data)
        if len(influxdb_data) != len(nodes):
            logger.error('_my_make_graphite_api_points_list: len(influxdb_data) != len(nodes)')
        if len(influxdb_data) == 1 and type(influxdb_data) is not list:
            influxdb_data = [ influxdb_data ]
        i=0
        while i < len(influxdb_data):
            _data[nodes[i].path] = [d['value'] for d in influxdb_data[i].get_points()]
            logger.debug('_my_make_graphite_api_points_list() %s: %s points', nodes[i].path, len(_data[nodes[i].path]))
            i += 1
        #logger.debug('_my_make_graphite_api_points_list() RET: %s', _data)
        return _data

    def fetch_multi(self, nodes, start_time, end_time):
        series = ', '.join(['"%s"' % node.path for node in nodes])
        # use the step of the node that is the most coarse
        # not sure if there's a better way? can we combine series
        # with different steps (and use the optimal step for each?)
        # probably not
        step_given = max([node.reader.step for node in nodes])
        step = step_given
        if self.config['min_data_points'] > 0:
            #let's do "adaptive" res based on configured min_data_points
            _step = (end_time - start_time) / self.config['min_data_points']
            if _step > step_given:
                #to reduce rounding errors always use a multiple of step_given
                step = int(floor(_step/step_given)*step_given)
        logger.debug("Calling influxdb multi fetch from %s to %s by %s: %s points", start_time, end_time, step, (end_time-start_time)/step)
        query = 'select mean(value) as value from %s where (time > %ds and time <= %ds) GROUP BY time(%ss)' % (
                series, start_time, end_time, step)
        query = '; '.join([self.create_fetch_query(node.path, start_time, end_time, step) for node in nodes])
        logger.debug('fetch_multi() query: %s', query)
        logger.debug('fetch_multi() - start_time: %s - end_time: %s, step %s',
                     datetime.datetime.fromtimestamp(float(start_time)), datetime.datetime.fromtimestamp(float(end_time)), step)

        with self.statsd_client.timer('service_is_graphite-api.ext_service_is_influxdb.target_type_is_gauge.unit_is_ms.action_is_select_datapoints'):
            logger.debug("Calling influxdb multi fetch with query - %s", query)
            data = self.client.query(query, params=_INFLUXDB_CLIENT_PARAMS)
        logger.debug('fetch_multi() - Retrieved %d result set(s)', len(data))
        logger.debug('fetch_multi() - Retrieved result type %s', type(data))
        data = self._my_make_graphite_api_points_list(data, nodes)
        logger.debug('fetch_multi() - Retrieved %d result set(s)', len(data))
        # some series we requested might not be in the resultset.
        # this is because influx doesn't include series that had no values
        # this is a behavior that some people actually appreciate when graphing, but graphite doesn't do this (yet),
        # and we want to look the same, so we must add those back in.
        # a better reason though, is because for advanced alerting cases like bosun, you want all entries even if they have no data, so you can properly
        # compare, join, or do logic with the targets returned for requests for the same data but from different time ranges, you want them to all
        # include the same keys.
        time_info = start_time, end_time, step
        #logger.debug('fetch_multi() - time_info %s', time_info)
        #logger.debug('fetch_multi() - data %s', data)
        return time_info, data
