# stdlib\
from functools import partial
import logging
import unittest

# 3rd
from mock import Mock

# project
from tests.checks.common import Fixtures


log = logging.getLogger(__name__)

WMISampler = None


# Thoughts
# Log WMI activity
# Mechanism to timeout
# Check when pywintypes.com_error are raised


def load_fixture(f, args):
    """
    Build a WMI query result from a file and given parameters.
    """
    properties = []

    # Build from file
    data = Fixtures.read_file(f)
    for l in data.splitlines():
        property_name, property_value = l.split(" ")
        properties.append(Mock(Name=property_name, Value=property_value))

    # Append extra information
    property_name, property_value = args
    properties.append(Mock(Name=property_name, Value=property_value))

    return [Mock(Properties_=properties)]


class MockWMIConnection(object):
    """
    Mocked WMI connection.
    Save connection parameters so it can be tested.
    """
    def __init__(self, wmi_conn_args):
        super(MockWMIConnection, self).__init__()
        self._wmi_conn_args = wmi_conn_args
        self._last_wmi_query = None

    def get_conn_args(self):
        """
        Return parameters used to set up the WMI connection.
        """
        return self._wmi_conn_args

    def get_last_wmi_query(self):
        """
        Return the last WMI query submitted via the WMI connection,
        """
        return self._last_wmi_query

    def ExecQuery(self, wql, *args, **kwargs):
        self._last_wmi_query = wql
        if wql == "Select AvgDiskBytesPerWrite,FreeMegabytes "\
                "from Win32_PerfFormattedData_PerfDisk_LogicalDisk":
            results = load_fixture("win32_perfformatteddata_perfdisk_logicaldisk", ("Name", "C:"))
            results += load_fixture("win32_perfformatteddata_perfdisk_logicaldisk", ("Name", "D:"))
            return results

        return []


class Counter(object):
    def __init__(self):
        self.value = 0

    def __iadd__(self, other):
        self.value += other
        return self

    def __eq__(self, other):
        return self.value == other

    def __str__(self):
        return str(self.value)

    def reset(self):
        self.value = 0


class MockDispatch(object):
    """
    Mock for win32com.client Dispatch class.
    """
    _connect_call_count = Counter()

    def __init__(self, *args, **kwargs):
        pass

    @classmethod
    def reset(cls):
        """
        FIXME - Dirty patch to reset `ConnectServer.call_count` to 0.
        """
        cls._connect_call_count.reset()

    def ConnectServer(self, *args, **kwargs):
        """
        Return a MockWMIConnection.
        """
        MockDispatch._connect_call_count += 1
        wmi_conn_args = (args, kwargs)
        return MockWMIConnection(wmi_conn_args)

    # FIXME - Return the _connect_call_count[0] directly
    ConnectServer.call_count = _connect_call_count


class TestUnitWMI(unittest.TestCase):
    """
    Unit tests for WMISampler.
    """
    def setUp(self):
        """
        Mock WMI related Python packages, so it can be tested on any environment.
        """
        import sys
        global WMISampler

        sys.modules['pywintypes'] = Mock()
        sys.modules['win32com'] = Mock()
        sys.modules['win32com.client'] = Mock(Dispatch=MockDispatch)

        # FIXME
        MockDispatch.reset()

        from checks.libs.wmi import sampler
        WMISampler = partial(sampler.WMISampler, log)

    def assertWMIConnWith(self, wmi_sampler, param):
        """
        Helper, assert that the WMI connection was established with the right parameter and value.
        """
        wmi_instance = wmi_sampler._get_connection()
        wmi_conn_args, wmi_conn_kwargs = wmi_instance.get_conn_args()
        if isinstance(param, tuple):
            key, value = param
            self.assertIn(key, wmi_conn_kwargs)
            self.assertEquals(wmi_conn_kwargs[key], value)
        else:
            self.assertIn(param, wmi_conn_args)

    def assertWMIQuery(self, wmi_sampler, wmi_query):
        """
        Helper, assert that the given WMI query was submitted.
        """
        wmi_instance = wmi_sampler._get_connection()
        last_wmi_query = wmi_instance.get_last_wmi_query()
        self.assertEquals(last_wmi_query, wmi_query)

    def assertWMIObject(self, wmi_obj, property_names):
        pass

    def test_raw_perf_properties(self):
        """
        Extend the list of properties to query for RAW Performance classes.
        """
        # Formatted Performance class
        wmi_sampler = WMISampler("Win32_PerfFormattedData_PerfOS_System", ["ProcessorQueueLength"])
        self.assertEquals(len(wmi_sampler.property_names), 1)

        # Raw Performance class
        wmi_sampler = WMISampler("Win32_PerfRawData_PerfOS_System", ["ProcessorQueueLength"])
        # self.assertEquals(len(wmi_sampler.property_names), 3)

    def test_wmi_connection(self):
        """
        Establish a WMI connection to the specified host/namespace, with the right credentials.
        """
        wmi_sampler = WMISampler(
            "Win32_PerfRawData_PerfOS_System",
            ["ProcessorQueueLength"],
            "myhost",
            "some/namespace",
            "datadog",
            "password"
        )
        wmi_conn = wmi_sampler._get_connection()

        # WMI connection is cached
        self.assertIn('myhost:some/namespace:datadog', wmi_sampler._wmi_connections)

        # Connection was established with the right parameters
        self.assertWMIConnWith(wmi_sampler, "myhost")
        self.assertWMIConnWith(wmi_sampler, "some/namespace")

    def test_wmi_connection_pooling(self):
        """
        Share WMI connections among WMISampler objects.
        """
        from win32com.client import Dispatch

        wmi_sampler_1 = WMISampler("Win32_PerfRawData_PerfOS_System", ["ProcessorQueueLength"])
        wmi_sampler_2 = WMISampler("Win32_OperatingSystem", ["TotalVisibleMemorySize"])
        wmi_sampler_3 = WMISampler("Win32_PerfRawData_PerfOS_System", ["ProcessorQueueLength"], "myhost")  # noqa

        wmi_sampler_1.sample()
        wmi_sampler_2.sample()

        self.assertEquals(Dispatch.ConnectServer.call_count, 1, Dispatch.ConnectServer.call_count)

        wmi_sampler_3.sample()

        self.assertEquals(Dispatch.ConnectServer.call_count, 2, Dispatch.ConnectServer.call_count)

    def test_wmi_query(self):
        """
        Query WMI using WMI Query Language (WQL).
        """
        wmi_sampler = WMISampler("Win32_PerfFormattedData_PerfDisk_LogicalDisk",
                                 ["AvgDiskBytesPerWrite", "FreeMegabytes"])
        wmi_sampler.sample()

        # Assert the WMI query
        self.assertWMIQuery(
            wmi_sampler,
            "Select AvgDiskBytesPerWrite,FreeMegabytes"
            " from Win32_PerfFormattedData_PerfDisk_LogicalDisk"
        )

    def test_wmi_parser(self):
        """
        Parse WMI objects from WMI query results.
        """
        wmi_sampler = WMISampler("Win32_PerfFormattedData_PerfDisk_LogicalDisk",
                                 ["AvgDiskBytesPerWrite", "FreeMegabytes"])
        wmi_sampler.sample()

        # Assert `results`
        expected_results = [
            {
                'freemegabytes': 19742.0,
                'name': 'C:',
                'avgdiskbytesperwrite': 1536.0
            }, {
                'freemegabytes': 19742.0,
                'name': 'D:',
                'avgdiskbytesperwrite': 1536.0
            }
        ]

        self.assertEquals(wmi_sampler, expected_results, wmi_sampler)

    def test_wmi_sampler_iterator(self):
        """
        Iterate on the WMISampler object iterates on its current sample.
        """
        wmi_sampler = WMISampler("Win32_PerfFormattedData_PerfDisk_LogicalDisk",
                                 ["AvgDiskBytesPerWrite", "FreeMegabytes"])
        wmi_sampler.sample()

        self.assertEquals(len(wmi_sampler), 2)

        for wmi_obj in wmi_sampler:
            self.assertWMIObject(wmi_obj, ["AvgDiskBytesPerWrite", "FreeMegabytes", "name"])


class TestIntegrationWMI(unittest.TestCase):
    """
    Integration tests for WMISampler.
    """
    pass
