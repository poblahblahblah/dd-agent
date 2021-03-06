# 3p
import simplejson as json

# project
from tests.checks.common import AgentCheckTest, Fixtures
from checks import AgentCheck

CPU = "CPU"
MEM = "MEM"
FS = "fs"
NET = "net"
NET_ERRORS = "net_errors"
DISK = "disk"
DISK_USAGE = "disk_usage"

METRICS = [
    ('kubernetes.memory.usage', MEM),
    ('kubernetes.filesystem.usage', FS),
    ('kubernetes.filesystem.usage_pct', FS),
    ('kubernetes.cpu.usage.total', CPU),
    ('kubernetes.network.tx_bytes', NET),
    ('kubernetes.network.rx_bytes', NET),
    ('kubernetes.network_errors', NET_ERRORS),
    ('kubernetes.diskio.io_service_bytes.stats.total', DISK),
    ('kubernetes.filesystem.usage_pct', DISK_USAGE),
    ('kubernetes.filesystem.usage', DISK_USAGE),
]

class TestKubernetes(AgentCheckTest):

    CHECK_NAME = 'kubernetes'

    def test_fail(self):
        # To avoid the disparition of some gauges during the second check
        mocks = {'_retrieve_json': lambda x: json.loads(Fixtures.read_file("metrics.json"))}
        config = {
            "instances": [{"host": "foo"}]
        }

        # Can't use run_check_twice due to specific metrics
        self.run_check(config, mocks=mocks, force_reload=True)
        self.assertServiceCheck("kubernetes.kubelet.check", status=AgentCheck.CRITICAL)

    def test_metrics(self):
        # To avoid the disparition of some gauges during the second check
        mocks = {'_retrieve_json': lambda x: json.loads(Fixtures.read_file("metrics.json"))}
        config = {
            "instances": [
                {
                    "host": "foo",
                    "enable_kubelet_checks": False
                }
            ]
        }



        # Can't use run_check_twice due to specific metrics
        self.run_check_twice(config, mocks=mocks, force_reload=True)

        expected_tags = [
            (['container_name:/kubelet', 'pod_name:no_pod'], [MEM, CPU, NET, DISK]),
            (['container_name:k8s_POD.e4cc795_propjoe-dhdzk_default_ba151259-36e0-11e5-84ce-42010af01c62_ef0ed5f9', 'pod_name:default/propjoe-dhdzk'], [MEM, CPU, FS, NET, NET_ERRORS]),
            (['container_name:/kube-proxy', 'pod_name:no_pod'], [MEM, CPU, NET]),
            (['container_name:k8s_POD.2688308a_kube-dns-v8-smhcb_kube-system_b80ffab3-3619-11e5-84ce-42010af01c62_295f14ff', 'pod_name:kube-system/kube-dns-v8-smhcb'], [MEM, CPU, FS, NET, NET_ERRORS]),
            (['container_name:/docker-daemon', 'pod_name:no_pod'], [MEM, CPU, DISK, NET]),
            (['container_name:k8s_etcd.2e44beff_kube-dns-v8-smhcb_kube-system_b80ffab3-3619-11e5-84ce-42010af01c62_e3e504ad', 'pod_name:kube-system/kube-dns-v8-smhcb'], [MEM, CPU, FS, NET, NET_ERRORS, DISK]),
            (['container_name:k8s_POD.e4cc795_fluentd-cloud-logging-kubernetes-minion-mu4w_kube-system_d0feac1ad02da9e97c4bf67970ece7a1_49dd977d', 'pod_name:kube-system/fluentd-cloud-logging-kubernetes-minion-mu4w'], [MEM, CPU, FS, NET, NET_ERRORS, DISK]),
            (['container_name:k8s_skydns.1e752dc0_kube-dns-v8-smhcb_kube-system_b80ffab3-3619-11e5-84ce-42010af01c62_7c1345a1', 'pod_name:kube-system/kube-dns-v8-smhcb'], [MEM, CPU, FS, NET, NET_ERRORS]),
            (['container_name:/', 'pod_name:no_pod'], [MEM, CPU, FS, NET, NET_ERRORS, DISK]),
            (['container_name:/system/docker', 'pod_name:no_pod'], [MEM, CPU, DISK, NET]),
            (['container_name:k8s_propjoe.21f63023_propjoe-dhdzk_default_ba151259-36e0-11e5-84ce-42010af01c62_19879457', 'pod_name:default/propjoe-dhdzk'], [MEM, CPU, FS, NET, NET_ERRORS]),
            (['container_name:/system', 'pod_name:no_pod'], [MEM, CPU, NET, DISK]),
            (['container_name:k8s_POD.3b46e8b9_kube-ui-v1-sv2sq_kube-system_b7e8f250-3619-11e5-84ce-42010af01c62_209ed1dc', 'pod_name:kube-system/kube-ui-v1-sv2sq'], [MEM, CPU, FS, NET, NET_ERRORS]),
            (['container_name:k8s_kube2sky.1afa6a47_kube-dns-v8-smhcb_kube-system_b80ffab3-3619-11e5-84ce-42010af01c62_624bc34c', 'pod_name:kube-system/kube-dns-v8-smhcb'], [MEM, CPU, FS, NET, NET_ERRORS]),
            (['container_name:k8s_POD.e4cc795_propjoe-lkc3l_default_3a9b1759-4055-11e5-84ce-42010af01c62_45d1185b', 'pod_name:default/propjoe-lkc3l'], [MEM, CPU, FS, NET, NET_ERRORS]),
            (['container_name:k8s_POD.e4cc795_haproxy-6db79c7bbcac01601ac35bcdb18868b3-rr7la_default_86527bf8-36cd-11e5-84ce-42010af01c62_5ad59bf3', 'pod_name:default/haproxy-6db79c7bbcac01601ac35bcdb18868b3-rr7la'], [MEM, CPU, FS, NET, NET_ERRORS]),
            (['container_name:k8s_haproxy.69b6303b_haproxy-6db79c7bbcac01601ac35bcdb18868b3-rr7la_default_86527bf8-36cd-11e5-84ce-42010af01c62_a35b9731', 'pod_name:default/haproxy-6db79c7bbcac01601ac35bcdb18868b3-rr7la'], [MEM, CPU, FS, NET, NET_ERRORS]),
            (['container_name:k8s_kube-ui.c17839c_kube-ui-v1-sv2sq_kube-system_b7e8f250-3619-11e5-84ce-42010af01c62_d2b9aa90', 'pod_name:kube-system/kube-ui-v1-sv2sq'], [MEM, CPU, FS, NET, NET_ERRORS]),
            (['container_name:k8s_propjoe.21f63023_propjoe-lkc3l_default_3a9b1759-4055-11e5-84ce-42010af01c62_9fe8b7b0', 'pod_name:default/propjoe-lkc3l'], [MEM, CPU, FS, NET, NET_ERRORS]),
            (['container_name:k8s_healthz.4469a25d_kube-dns-v8-smhcb_kube-system_b80ffab3-3619-11e5-84ce-42010af01c62_241c34d1', 'pod_name:kube-system/kube-dns-v8-smhcb'], [MEM, CPU, FS, NET, NET_ERRORS, DISK]),
            (['container_name:k8s_fluentd-cloud-logging.7721935b_fluentd-cloud-logging-kubernetes-minion-mu4w_kube-system_d0feac1ad02da9e97c4bf67970ece7a1_2c3c0879', 'pod_name:kube-system/fluentd-cloud-logging-kubernetes-minion-mu4w'], [MEM, CPU, FS, NET, NET_ERRORS, DISK]),
            (['container_name:dd-agent', 'pod_name:no_pod'], [MEM, CPU, FS, NET, NET_ERRORS, DISK])
        ]
        for m, _type in METRICS:
            for tags, types in expected_tags:
                if _type in types:
                    self.assertMetric(m, count=1, tags=tags)

        self.coverage_report()

    def test_historate(self):
        # To avoid the disparition of some gauges during the second check
        mocks = {'_retrieve_json': lambda x: json.loads(Fixtures.read_file("metrics.json"))}
        config = {
            "instances": [
                {
                    "host": "foo",
                    "enable_kubelet_checks": False,
                    "use_histogram": True,
                }
            ]
        }

        # Can't use run_check_twice due to specific metrics
        self.run_check_twice(config, mocks=mocks, force_reload=True)

        metric_suffix = ["count", "avg", "median", "max", "95percentile"]

        expected_tags = [
            (['pod_name:no_pod'], [MEM, CPU, NET, DISK, DISK_USAGE, NET_ERRORS]),
            (['pod_name:default/propjoe-dhdzk'], [MEM, CPU, FS, NET, NET_ERRORS]),
            (['pod_name:kube-system/kube-dns-v8-smhcb'], [MEM, CPU, FS, NET, NET_ERRORS, DISK]),
            (['pod_name:kube-system/fluentd-cloud-logging-kubernetes-minion-mu4w'], [MEM, CPU, FS, NET, NET_ERRORS, DISK]),
            (['pod_name:kube-system/kube-dns-v8-smhcb'], [MEM, CPU, FS, NET, NET_ERRORS]),
            (['pod_name:default/propjoe-dhdzk'], [MEM, CPU, FS, NET, NET_ERRORS]),
            (['pod_name:kube-system/kube-ui-v1-sv2sq'], [MEM, CPU, FS, NET, NET_ERRORS]),
            (['pod_name:default/propjoe-lkc3l'], [MEM, CPU, FS, NET, NET_ERRORS]),
            (['pod_name:default/haproxy-6db79c7bbcac01601ac35bcdb18868b3-rr7la'], [MEM, CPU, FS, NET, NET_ERRORS]),
        ]

        for m, _type in METRICS:
            for m_suffix in metric_suffix:
                for tags, types in expected_tags:
                    if _type in types:
                        self.assertMetric("{0}.{1}".format(m, m_suffix), count=1, tags=tags)

        self.coverage_report()
