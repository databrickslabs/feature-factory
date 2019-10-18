from framework.configobj import ConfigObj
from datetime import date, datetime, timedelta
import calendar
from pyspark.sql.functions import col
from dateutil.relativedelta import relativedelta

class DateTimeManager:
    def __init__(self, _snapshot_date, _dt_col: str, _dt_format: str, _date_format: str,
                 _config: ConfigObj, _partition_col=None, _partition_dt_format= None):
        """
        :param _dt_col: datetime column if relevant, otherwise just use date column
        :param _dt_format: common datetime format in python str format
        this is generally the column in the date_dim that is ued throughout the codebase
        any code using a different format will need to use the clean_time function
        :param _date_format: common date format in python str format
        :param _partition_dt_format: python format string for the datetime format used for partition col
        This only works if the partition col is date type
        :param _config: config object passed in
        :param _partition_col: partition column if dataset[s] are partitioned on date/datetime
        :param _partition_period: Supported is Daily, Monthly, Yearly,
        this will select the first day of the period based on the _partition_dt_format
        to ensure the entire period is included in the partition filters
        """
        self.date_time_col = _dt_col
        self.date_time_format = _dt_format
        self.date_format = _date_format
        self.partition_col = _partition_col
        self.partition_dt_format = _partition_dt_format
        self.partition_range = {}
        self.config = _config
        self.dt_tracker = ConfigObj()

        if self.config.contains("time_helpers.snapshot_date"):
            self.snapshot_date = self.config.get_or_else("time_helpers.snapshot_date", "")
        elif _snapshot_date is None and not self.config.contains("time_helpers.snapshot_date"):
            self.snapshot_date = date.today().strftime(_date_format)
        else:
            self.snapshot_date = _snapshot_date

        self.snapshot_date_dt = datetime.strptime(self.snapshot_date, self.date_format).date()
        self.end = self.snapshot_date_dt.strftime(self.date_format)
        self.all_dates = set([])

        if not self.config.contains("time_helpers.snapshot_type"):
            mo_max_day = calendar.monthrange(self.snapshot_date_dt.year, self.snapshot_date_dt.month)[1]
            snapshot_type = "MONTH" if mo_max_day == self.snapshot_date_dt.day else "DAILY"
            self.config.add("time_helpers.snapshot_type", snapshot_type)

        self.config.add("time_helpers.snapshot_date", self.snapshot_date)
        self.config.add("time_helpers.partition_col", self.partition_col)
        self.config.add("time_helpers.date_col", self.date_time_col)
        self.config.add("time_helpers.date_col_format", self.date_format)
        self.config.add("time_helpers.partition_col_format", self.partition_dt_format)

    def get_config(self):
        return self.config

    def _track_date(self, period, dt):
        if not self.dt_tracker.contains("{}.all_dates".format(period)):
            self.dt_tracker.add("{}.all_dates".format(period), [dt])
        else:
            dl = self.dt_tracker.get_or_else("{}.all_dates".format(period), "")
            self.dt_tracker.add("{}.all_dates".format(period), dl + [dt])

    def _append_ranges(self):
        for period, dates in self.dt_tracker.configs.items():
            mm = self._get_min_max_dt_from_list(dates['all_dates'], self.date_format)
            self.dt_tracker.add("{}.min".format(period), mm[0].strftime(self.date_format))
            self.dt_tracker.add("{}.max".format(period), mm[1].strftime(self.date_format))

    def _append_trend_ranges(self, trend_period_ranges):
        for rng in trend_period_ranges: # [["1m","12m"]]
            num_range = list(range(int(rng[0][:-1]), int(rng[1][:-1])+1))
            period_type = rng[0][-1:]
            trend_period = ["{}{}".format(num, period_type) for num in num_range]
            self.append_periods(trend_period, "time_helpers.trend_filters.{}".format(period_type))

    def append_periods(self, periods, cfg_prefix="time_helpers.date_filters"):

        date_format = self.date_format
        config = self.config
        self.all_dates.add(self.end)

        def _month(_length):
            start = (self.snapshot_date_dt - relativedelta(months=int(_length))).strftime(date_format)
            self.all_dates.add(start)
            self._track_date("m", start)
            period_config = {
                "start": start,
                "end": self.end
            }
            config.add("{}.ranges.{}m".format(cfg_prefix,_length), period_config)
            return True

        def _week(_length):
            start = (self.snapshot_date_dt - relativedelta(weeks=int(_length))).strftime(date_format)
            self.all_dates.add(start)
            self._track_date("w", start)
            period_config = {
                "start": start,
                "end": self.end
            }
            config.add("{}.ranges.{}w".format(cfg_prefix,_length), period_config)
            return True

        def _day(_length):
            start = (self.snapshot_date_dt - relativedelta(days=int(_length))).strftime(date_format)
            self.all_dates.add(start)
            self._track_date("d", start)
            period_config = {
                "start": start,
                "end": self.end
            }
            config.add("{}.ranges.{}d".format(cfg_prefix,_length), period_config)
            return True

        def _year(_length):
            start = (self.snapshot_date_dt - relativedelta(years=int(_length))).strftime(date_format)
            self.all_dates.add(start)
            self._track_date("y", start)
            period_config = {
                "start": start,
                "end": self.end
            }
            config.add("{}.ranges.{}y".format(cfg_prefix,_length), period_config)
            return True

        for p in periods:
            assert ("m" in p[-1:] or "w" in p[-1:] or "d" in p[-1:] or "y" in p[-1:]), \
                "Only m,w,d,y accepted as period types for insertion"

        for period in periods:
            length = period[:-1]
            period_type = period[-1:]

            append = {
                "m": _month,
                "w": _week,
                "d": _day,
                "y": _year
            }

            append[period_type](length)

            # Track the periods that were appended for later use
            if not self.dt_tracker.contains("{}.periods".format(period_type)):
                self.dt_tracker.add("{}.periods".format(period_type), [int(length)])
            else:
                pl = self.dt_tracker.get_or_else("{}.periods".format(period_type), "")
                self.dt_tracker.add("{}.periods".format(period_type), pl + [int(length)])
        self._append_ranges()

        # self._build_numerical_ranges()

        # If config is missing min/max partition values, insert them
        # Otherwise set the min/max of this to what's in the config
        all_dates_mm = self._get_min_max_dt_from_list(self.all_dates, date_format)
        if not config.contains("time_helpers.partition_lower"):
            self.partition_range['start'] = all_dates_mm[0].strftime(self.partition_dt_format)
            config.add("time_helpers.partition_lower", self.partition_range['start'])
        else:
            self.partition_range['start'] = config.get_or_else("time_helpers.partition_lower",
                                                               "partition_lower is not set")
        if not config.contains("time_helpers.partition_upper"):
            self.partition_range['end'] = all_dates_mm[1].strftime(self.partition_dt_format)
            config.add("time_helpers.partition_upper", self.partition_range['end'])
        else:
            self.partition_range['end'] = config.get_or_else("time_helpers.partition_upper",
                                                             "partition_upper is not set")

    def scoped_partition_filter(self, range_dict=None, start=None, end=None, partition_col=None,
                                input_fmt=None, partition_col_fmt=None, snapshot_type=None):

        if snapshot_type is None:
            snapshot_type = self.config.get_or_else("time_helpers.snapshot_type", "MONTH")

        assert (snapshot_type == "MONTH" or snapshot_type == "DAILY"), \
            "snapshot_type must be DAILY or MONTH, got {}".format(snapshot_type)

        if input_fmt is None:
            fmt = self.date_format
        else:
            fmt = input_fmt

        if partition_col_fmt is None:
            partition_col_fmt = self.partition_dt_format

        if range_dict is not None and start is None and end is None:
            min_date_dt = datetime.strptime(range_dict['start'], fmt)
            max_date_dt = datetime.strptime(range_dict['end'], fmt)

        if start is None and range_dict is None:
            min_date_dt = min([datetime.strptime(dt, fmt) for dt in list(self.all_dates)])
        elif range_dict is None and start is not None:
            min_date_dt = datetime.strptime(start, fmt)

        if end is None and range_dict is None:
            max_date_dt = max([datetime.strptime(dt, fmt) for dt in list(self.all_dates)])
        elif range_dict is None and end is not None:
            max_date_dt = datetime.strptime(end, fmt)

        if partition_col is None:
            partition_col = self.config.get_or_else("time_helpers.partition_col",
                                                    "No time_helpers.partition_col specified in config and "
                                                    "no partition column passed into function")

        if snapshot_type == "MONTH":
            adj_start = (min_date_dt - relativedelta(months=1 - 1)).replace(day=1)
            part_filter = col(partition_col).between(adj_start.strftime(partition_col_fmt),
                                                     max_date_dt.strftime(partition_col_fmt))
        else:
            part_filter = col(partition_col).between(min_date_dt.strftime(partition_col_fmt),
                                                     max_date_dt.strftime(partition_col_fmt))

        return part_filter

    def scoped_time_filter(self, range_dict=None, start=None, end=None, _fmt=None):

        if _fmt is None:
            fmt = self.date_format
        else:
            fmt = _fmt

        if range_dict is not None and start is None and end is None:
            min_date_dt = datetime.strptime(range_dict['start'], fmt)
            max_date_dt = datetime.strptime(range_dict['end'], fmt)

        if start is None and range_dict is None:
            min_date_dt = min([datetime.strptime(dt, fmt) for dt in list(self.all_dates)])
        elif range_dict is None and start is not None:
            min_date_dt = datetime.strptime(start, fmt)

        if end is None and range_dict is None:
            max_date_dt = max([datetime.strptime(dt, fmt) for dt in list(self.all_dates)])
        elif range_dict is None and end is not None:
            max_date_dt = datetime.strptime(end, fmt)

        min_date_str = min_date_dt.strftime(fmt)
        max_date_str = max_date_dt.strftime(fmt)
        dt_filter = col(self.date_time_col).between(min_date_str, max_date_str)

        if self.partition_col is not None:
            snapshot_type = self.config.get_or_else("time_helpers.snapshot_type", "MONTH")
            partition_filter = self.scoped_partition_filter(min_date_dt,
                                                            max_date_dt,
                                                            self.partition_col,
                                                            self.partition_dt_format,
                                                            snapshot_type)
            dt_filter = dt_filter & partition_filter

        return dt_filter

    def _get_min_max_dt_from_list(self, l, fmt):
        mi = min([datetime.strptime(dt, fmt) for dt in l])
        ma = max([datetime.strptime(dt, fmt) for dt in l])
        return [mi, ma]
