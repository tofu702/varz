import utils

class SamplerStats(object):
  
  def __init__(self, sampler_data):
    '''Assumes that sampler_data is an SAMPLER_JSON object, per the protocol definition'''
    self.sampler_data = sampler_data

  def last_minute_stats(self):
    return self._compute_order_statistics(self.sampler_data["last_minute_samples"]["sample_values"])

  def all_time_stats(self):
    return self._compute_order_statistics(self.sampler_data["all_time_samples"]["sample_values"])

  def last_hour_stats(self):
    last_hour_data = self._filter_last_n_seconds(sample_set=self.sampler_data["all_time_samples"],
                                                 end_time=self.sampler_data["latest_time_sec"],
                                                 num_sec_before_end=3600)
    return self._compute_order_statistics(last_hour_data)

  def _compute_order_statistics(self, samples):
    '''Compute the median, 95th percentile
    Arguments
      samples: An array of integers [sample1, sample2, ...]
    Returns: {'median': #, 'percentile_95': 95th_percentile}'''
    if not samples:
      return {"median": 0, "percentile_95": 0}
    sorted_samples = sorted(samples)
    num_samples = len(sorted_samples)
    pos_95 = (num_samples * 95) / 100
    median = sorted_samples[num_samples/2]
    percentile_95 = sorted_samples[pos_95]
    return {"median": median, "percentile_95": percentile_95}

  def _filter_last_n_seconds(self, sample_set, end_time, num_sec_before_end):
    '''Filter the provided samples array for samples up to num_sec after the supplied end_time
    Arguments
      sample_set: A sample set with {"sample_values":..., "sample_times_sec":..., "samples_size":...
      end_time: The end time under consideration (in ms since the epoch)
      num_sec: How many seconds before the end time we which to keep
    Returns: An array of sample_values [sample1, sample2...]'''
    min_time = end_time - num_sec_before_end
    sample_values = sample_set["sample_values"]
    sample_times = sample_set["sample_times_sec"]
    return [sample_values[i] for i, x in enumerate(sample_times) if end_time >= x > min_time]


class CounterStats(object):
  def __init__(self, counter_data):
    '''Assumes that the counter_data is the COUNTER_JSON object per the protocol definition'''
    self.counter_data = counter_data

  def last_minute_count(self):
    # Compute which minute the last one is and use it's value
    last_min = utils.sec_since_epoch_to_datetime(self.counter_data["latest_time_sec"]).minute
    return self.counter_data["min_counters"][last_min]

  def last_hour_count(self):
    # TODO: Better handling of partial hours
    return sum(self.counter_data["min_counters"])

  def all_time_count(self):
    return self.counter_data["all_time_count"]
