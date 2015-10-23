# stdlib
from collections import namedtuple

# project
from checks import AgentCheck
from checks.libs.wmi.sampler import WMISampler

WMIMetric = namedtuple('WMIMetric', ['name', 'value', 'tags'])


class InvalidWMIQuery(Exception):
    """
    Invalid WMI Query.
    """
    pass


class MissingTagBy(Exception):
    """
    WMI query returned multiple rows but no `tag_by` value was given.
    """
    pass


class WMIAlternativeCheck(AgentCheck):
    """
    An alternative to Datadog agent WMI check.

    Windows only.
    """
    def __init__(self, name, init_config, agentConfig, instances):
        AgentCheck.__init__(self, name, init_config, agentConfig, instances)
        self.wmi_samplers = {}
        self.wmi_props = {}

    def check(self, instance):
        """
        Fetch WMI metrics.
        """
        # Connection information
        host = instance.get('host', "localhost")
        namespace = instance.get('namespace', "root\\cimv2")
        username = instance.get('username', "")
        password = instance.get('password', "")

        # WMI instance
        wmi_class = instance.get('class')
        metrics = instance.get('metrics')
        filters = instance.get('filters')
        tag_by = instance.get('tag_by', "").lower()

        # Create or retrieve an existing WMISampler
        instance_key = self._get_instance_key(host, namespace, wmi_class)

        metric_name_and_type_by_property, properties = \
            self._get_wmi_properties(instance_key, metrics)

        wmi_sampler = self._get_wmi_sampler(
            instance_key,
            wmi_class, properties,
            filters=filters,
            host=host,
            namespace=namespace,
            username=username,
            password=password
        )

        # Sample, extract & submit metrics
        wmi_sampler.sample()
        metrics = self._extract_metrics(wmi_sampler, tag_by)
        self._submit_metrics(metrics, metric_name_and_type_by_property)

    def _extract_metrics(self, wmi_sampler, tag_by):
        """
        Extract and tag metrics from the WMISampler.

        Raise when multiple WMIObject were returned by the sampler with no `tag_by` specified.

        Returns: List of WMIMetric
        ```
        [
            WMIMetric("freemegabytes", 19742, ["name:_total"]),
            WMIMetric("avgdiskbytesperwrite", 1536, ["name:c:"]),
        ]
        """
        if len(wmi_sampler) > 1 and not tag_by:
            raise MissingTagBy

        metrics = []

        for wmi_obj in wmi_sampler:
            tags = []
            for wmi_property, wmi_value in wmi_obj.iteritems():
                if wmi_property == tag_by:
                    tags.append(
                        "{name}:{value}".format(
                            name=tag_by.lower(),
                            value=wmi_value.lower()
                        )
                    )
                    continue
                try:
                    metrics.append(WMIMetric(wmi_property, float(wmi_value), tags))
                except ValueError:
                    self.log.warning(u"When extracting metrics with WMI, found a non digit value"
                                     " for property '{0}'.".format(wmi_property))
                    continue
        return metrics

    def _submit_metrics(self, metrics, metric_name_and_type_by_property):
        """
        Resolve metric name and type and submit it.
        """
        for metric in metrics:
            metric_name, metric_type = metric_name_and_type_by_property[metric.name]
            try:
                func = getattr(self, metric_type)
            except AttributeError:
                raise Exception(u"Invalid metric type: {0}".format(metric_type))

            func(metric_name, metric.value, metric.tags)

    def _get_instance_key(self, host, namespace, wmi_class):
        """
        Return an index key for a given instance. Usefull for caching.
        """
        return "{host}:{namespace}:{wmi_class}".format(
            host=host,
            namespace=namespace,
            wmi_class=wmi_class
        )

    def _get_wmi_sampler(self, instance_key, wmi_class, properties, **kwargs):
        """
        Create and cache a WMISampler for the given (class, properties)
        """
        if instance_key not in self.wmi_samplers:
            wmi_sampler = WMISampler(self.log, wmi_class, properties, **kwargs)
            self.wmi_samplers[instance_key] = wmi_sampler

        return self.wmi_samplers[instance_key]

    def _get_wmi_properties(self, instance_key, metrics):
        """
        Create and cache a (metric name, metric type) by WMI property map and a property list.
        """
        if instance_key not in self.wmi_props:
            metric_name_by_property = {
                wmi_property.lower(): (metric_name, metric_type)
                for wmi_property, metric_name, metric_type in metrics
            }
            properties = map(lambda x: x[0], metrics)
            self.wmi_props[instance_key] = (metric_name_by_property, properties)

        return self.wmi_props[instance_key]



    # def _query_wmi(self, w, wmi_class, wmi_properties, filters, tag_by):
    #     """
    #     Query WMI using WMI Query Language (WQL).

    #     Returns: List of unknown COMObject.
    #     """
    #     formated_wmi_properties = ",".join(wmi_properties)
    #     wql = "Select {properties} from {wmi_class}{filters}".format(
    #         properties=formated_wmi_properties,
    #         wmi_class=wmi_class,
    #         filters=self._format_filter(filters)
    #     )
    #     self.log.debug(u"Querying WMI: {0}".format(wql))

    #     results = w.ExecQuery(wql)

    #     try:
    #         self._raise_for_invalid(results, tag_by)
    #     except InvalidWMIQuery:
    #         self.log.warning(u"Invalid WMI query: {0}".format(wql))
    #         results = []
    #     except MissingTagBy:
    #         raise Exception(u"WMI query returned multiple rows but no `tag_by` value was given. "
    #                         "class={wmi_class} - properties={wmi_properties}".format(
    #                             wmi_class=wmi_class,
    #                             wmi_properties=wmi_properties
    #                         )
    #                         )

    #     return results

    # def _extract_metrics(self, results, tag_by):
    #     """
    #     Extract, parse and tag metrics from WMI query results.

    #     Returns: List of WMIMetric
    #     ```
    #     [
    #         WMIMetric("FreeMegabytes", 19742, ["name:_total"]),
    #         WMIMetric("AvgDiskBytesPerWrite", 1536, ["name:c:"]),
    #     ]
    #     ```
    #     """
    #     metrics = []
    #     for res in results:
    #         tags = []
    #         for wmi_property in res.Properties_:
    #             if wmi_property.Name == tag_by:
    #                 tags.append(
    #                     "{name}:{value}".format(
    #                         name=tag_by.lower(),
    #                         value=str(wmi_property.Value).lower()
    #                     )
    #                 )
    #                 continue
    #             try:
    #                 metrics.append(WMIMetric(wmi_property.Name, float(wmi_property.Value), tags))
    #             except ValueError:
    #                 self.log.warning(u"When extracting metrics with WMI, found a non digit value"
    #                                  " for property '{0}'.".format(wmi_property.Name))
    #                 continue
    #     return metrics

    # def _submit_metrics(self, metrics, metric_name_and_type_by_property):
    #     """
    #     Submit metrics to Datadog with the right name and type.
    #     """
    #     for metric in metrics:

    #         metric_key = (metric.name).lower()
    #         metric_name, metric_type = metric_name_and_type_by_property[metric_key]

    #         try:
    #             func = getattr(self, metric_type)
    #         except AttributeError:
    #             raise Exception(u"Invalid metric type: {0}".format(metric_type))

    #         func(metric_name, metric.value, metric.tags)

    # @staticmethod
    # def _raise_for_invalid(result, tag_by):
    #     """
    #     Raise:
    #     * `InvalidWMIQuery`: when the result returned by the WMI query is invalid
    #     * `MissingTagBy`: when the result returned by tge WMI query contains multiple rows
    #         but no `tag_by` value was given
    #     """
    #     try:
    #         if len(result) > 1 and not tag_by:
    #             raise MissingTagBy
    #     except (pywintypes.com_error):
    #         raise InvalidWMIQuery
