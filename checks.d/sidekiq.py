import re
from collections import namedtuple

# project
from checks import AgentCheck, CheckException

# 3rd party
import psutil
import redis

SidekiqStat = namedtuple("SidekiqStat", "name command key")

class Sidekiq(AgentCheck):
    APP_STATS = [
        SidekiqStat('processed', 'get', 'stat:processed'),
        SidekiqStat('failed', 'get', 'stat:failed'),
        SidekiqStat('scheduled', 'zcard', 'schedule'),
        SidekiqStat('retries', 'zcard', 'retry'),
        SidekiqStat('dead', 'zcard', 'dead')
    ]

    APP_PREFIX = 'sidekiq.app.'

    def __init__(self, name, init_config, agentConfig):
        AgentCheck.__init__(self, name, init_config, agentConfig)
        self.connections = {}

    def _procs_by_name(self):
        """
        Search for running sidekiq processes and group them by app name
        (i.e. `pgrep -fa '^sidekiq' | cut -d' ' -f 4`)
        """
        all_sidekiq_procs = [p for p in psutil.process_iter()
                             if p.name() == 'ruby'
                             and p.cmdline()[0].startswith('sidekiq')]

        sidekiq_procs_by_name = {}

        for sidekiq_proc in all_sidekiq_procs:
            # sidekiq overwrites command line to be e.g.
            #'sidekiq 3.5.3 myapp [2 of 10 busy]'
            # see: http://git.io/57ktWQ
            sk_cmdline = sidekiq_proc.cmdline()[0]
            app_name = sk_cmdline.split()[2]

            # sometimes there is no name....
            if re.search(r'^\[\d', app_name):
                app_name = None

            sidekiq_procs_by_name.setdefault(app_name, []).append(sidekiq_proc)

        return sidekiq_procs_by_name

    def _namespaced_key(self, namespace, key):
        if not namespace:
            return key
        else:
            return ":".join([namespace, key])

    def check(self, instance):
        """
        Reports stats as defined in Sidekiq::Stats (in http://git.io/e-QGLw ) as
        well as busy (as returned by sidekiq_web /dashboard/stats endpoint)
        """
        procs_by_name = self._procs_by_name()

        name = instance.get('name')
        if name == None and len(procs_by_name.keys()) == 1:
            name = procs_by_name.keys()[0]

        app_tags = []
        if name != None:
            app_tags.append('sidekiq:%s' % name)
        app_tags.extend(instance['tags'])

        worker_procs = procs_by_name.get(name, [])
        running_proc = next((p for p in worker_procs if p.is_running()), None)
        if name and not running_proc:
            self.warning("No running sidekiq workers matching name '%s'" % name)
            self.service_check('sidekiq.workers_running',
                               AgentCheck.CRITICAL, tags=app_tags)
        else:
            self.service_check('sidekiq.workers_running',
                               AgentCheck.OK, tags=app_tags)

        redis_url = instance.get('redis_url')
        conn = self.connections.get(redis_url) or redis.from_url(redis_url)
        namespace = instance.get('redis_namespace')

        #per-app stats
        for stat in self.APP_STATS:
            stat_name = self.APP_PREFIX + stat.name
            redis_command = getattr(conn, stat.command)
            key_name = self._namespaced_key(namespace, stat.key)

            self.gauge(stat_name, float(redis_command(key_name)), tags=app_tags)

        #calculate and report number of busy workers
        processes_key = self._namespaced_key(namespace, 'processes')
        processes = conn.smembers(processes_key)
        pipe = conn.pipeline()
        for process in processes:
            pipe.hget(self._namespaced_key(namespace, process), 'busy')
        busy = sum([int(num) for num in pipe.execute() if num is not None])

        self.gauge(self.APP_PREFIX + 'busy', float(busy), tags=app_tags)

        #individual queue stats and sum thereof
        enqueued = 0
        queues = conn.smembers(self._namespaced_key(namespace, 'queues'))
        for queue in queues:
            key_name = 'queue:%s' % queue
            queue_tags = app_tags + ['sidekiq_' + key_name]

            msgs = conn.llen(self._namespaced_key(namespace, key_name))
            self.gauge('sidekiq.queue.messages', float(msgs), tags=queue_tags)

            enqueued += msgs

        self.gauge(self.APP_PREFIX + 'enqueued', float(enqueued), tags=app_tags)

