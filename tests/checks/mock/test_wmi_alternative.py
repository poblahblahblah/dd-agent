# project
from tests.checks.common import AgentCheckTest
from tests.core.test_wmi import TestCommonWMI


class WMITestCase(AgentCheckTest, TestCommonWMI):
    CHECK_NAME = 'wmi_alternative_check'

    WMI_CONNECTION_CONFIG = {
        'host': "myhost",
        'namespace': "some/namespace",
        'username': "datadog",
        'password': "datadog",
        'class': "Win32_OperatingSystem",
        'metrics': [["NumberOfProcesses", "system.proc.count", "gauge"],
                    ["NumberOfUsers", "system.users.count", "gauge"]]
    }

    WMI_CONFIG = {
        'class': "Win32_PerfFormattedData_PerfDisk_LogicalDisk",
        'metrics': [["AvgDiskBytesPerWrite", "winsys.disk.avgdiskbytesperwrite", "gauge"],
                    ["FreeMegabytes", "winsys.disk.freemegabytes", "gauge"]],
        'tag_by': "Name"
    }

    WMI_CONFIG_NO_TAG_BY = {
        'class': "Win32_PerfFormattedData_PerfDisk_LogicalDisk",
        'metrics': [["AvgDiskBytesPerWrite", "winsys.disk.avgdiskbytesperwrite", "gauge"],
                    ["FreeMegabytes", "winsys.disk.freemegabytes", "gauge"]],
    }

    WMI_CONFIG_FILTER = {
        'class': "Win32_PerfFormattedData_PerfDisk_LogicalDisk",
        'metrics': [["AvgDiskBytesPerWrite", "winsys.disk.avgdiskbytesperwrite", "gauge"],
                    ["FreeMegabytes", "winsys.disk.freemegabytes", "gauge"]],
        'filters': [{'Name': "C:"}]
    }

    def _get_wmi_sampler(self):
        """
        Helper to easily retrieve, if exists and unique, the WMISampler created
        by the configuration.

        Fails when multiple samplers are avaiable.
        """
        self.assertTrue(
            self.check.wmi_samplers,
            u"Unable to retrieve the WMISampler: no sampler was found"
        )
        self.assertEquals(
            len(self.check.wmi_samplers), 1,
            u"Unable to retrieve the WMISampler: expected a unique, but multiple were found"
        )

        return self.check.wmi_samplers.itervalues().next()

    def test_wmi_connection(self):
        """
        Establish a WMI connection to the specified host/namespace, with the right credentials.
        """
        # Run check
        config = {
            'instances': [self.WMI_CONNECTION_CONFIG]
        }
        self.run_check(config)

        # A WMISampler is cached
        self.assertIn("myhost:some/namespace:Win32_OperatingSystem", self.check.wmi_samplers)
        wmi_sampler = self.check.wmi_samplers["myhost:some/namespace:Win32_OperatingSystem"]

        # Connection was established with the right parameters
        self.assertWMIConnWith(wmi_sampler, "myhost")
        self.assertWMIConnWith(wmi_sampler, "some/namespace")

    def test_wmi_properties(self):
        """
        Compute a (metric name, metric type) by WMI property map and a property list.
        """
        # Set up the check
        config = {
            'instances': [self.WMI_CONNECTION_CONFIG]
        }
        self.run_check(config)

        # WMI props are cached
        self.assertIn("myhost:some/namespace:Win32_OperatingSystem", self.check.wmi_props)
        metric_name_and_type_by_property, properties = \
            self.check.wmi_props["myhost:some/namespace:Win32_OperatingSystem"]

        # Assess
        self.assertEquals(
            metric_name_and_type_by_property,
            {
                'numberofprocesses': ("system.proc.count", "gauge"),
                'numberofusers': ("system.users.count", "gauge")
            }
        )
        self.assertEquals(properties, ["NumberOfProcesses", "NumberOfUsers"])

    def test_metric_extraction(self):
        """
        Extract metrics from WMI query results.
        """
        # Set up the check
        config = {
            'instances': [self.WMI_CONFIG]
        }
        self.run_check(config)

        # Retrieve the sampler
        wmi_sampler = self._get_wmi_sampler()

        # Extract metrics
        metrics = self.check._extract_metrics(wmi_sampler, "name")

        # Assess
        WMIMetric = self.load_class("WMIMetric")
        expected_metrics = [
            WMIMetric("freemegabytes", 19742, ["name:c:"]),
            WMIMetric("avgdiskbytesperwrite", 1536, ["name:c:"]),
            WMIMetric("freemegabytes", 19742, ["name:d:"]),
            WMIMetric("avgdiskbytesperwrite", 1536, ["name:d:"]),
        ]

        self.assertEquals(metrics, expected_metrics)

    def test_mandatory_tag_by(self):
        """
        Exception is raised when the result returned by tge WMI query contains multiple rows
        but no `tag_by` value was given.
        """
        config = {
            'instances': [self.WMI_CONFIG_NO_TAG_BY]
        }
        with self.assertRaises(Exception):
            self.run_check(config)

    def test_check(self):
        """
        Assess check coverage.
        """
        # Run the check
        config = {
            'instances': [self.WMI_CONFIG]
        }
        self.run_check(config)

        for _, mname, _ in self.WMI_CONFIG['metrics']:
            self.assertMetric(mname, count=2)

        self.coverage_report()
