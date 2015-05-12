import utils

# TODO: Fix all these to take the current time and compute stats accordingly
# For minutes this entails possibly returning 0 if the current minute is not the one we are looking at
## We should also use the last minute, not the current minute
# For hours, this means zeroing all data between the last sample and the current time

class SamplerStats(object):
  
  def __init__(self, sampler_data, current_epoch_sec):
    '''Assumes that sampler_data is an SAMPLER_JSON object, per the protocol definition'''
    self.sampler_data = sampler_data
    self.current_epoch_sec = current_epoch_sec
    self.current_min = utils.epoch_sec_to_minutes_since_epoch(self.current_epoch_sec)

  def last_minute_stats(self):
    # TODO: Lots of changes need to happen to make this the last full clock time minute
    latest_min  = utils.epoch_sec_to_minutes_since_epoch(self.sampler_data["latest_time_sec"])
    if latest_min != self.current_min:
      stats = self._compute_order_statistics([])
      stats["count"] = 0
    else:
      last_minute_samples = self.sampler_data["last_minute_samples"]
      stats = self._compute_order_statistics(last_minute_samples["sample_values"])
      stats["count"] = last_minute_samples["num_events"]
    return stats

  def all_time_stats(self):
    all_time_samples = self.sampler_data["all_time_samples"]
    stats = self._compute_order_statistics(all_time_samples["sample_values"])
    stats["count"] = all_time_samples["num_events"]
    return stats

  def last_hour_stats(self):
    all_time_samples = self.sampler_data["all_time_samples"]
    last_hour_data = self._filter_last_n_seconds(sample_set=all_time_samples,
                                                 end_time=self.current_epoch_sec,
                                                 num_sec_before_end=3600)
    stats = self._compute_order_statistics(last_hour_data)
    stats["count"] = self._estimate_num_events_in_last_hour(all_time_samples, len(last_hour_data))
    return stats

  def _estimate_num_events_in_last_hour(self, all_time_samples, num_last_hour_samples):
    num_all_time_events = all_time_samples["num_events"]
    num_total_all_time_samples = all_time_samples["samples_size"]
    # Multiply then divide so we can do this entirely with ints
    return (num_all_time_events * num_last_hour_samples) / num_total_all_time_samples

  def _compute_order_statistics(self, samples):
    '''Compute the median, quartiles, 95th percentile and largest_value (largest sample value)
    Arguments
      samples: An array of integers [sample1, sample2, ...]
    Returns: {'quartile_1': #, 'median': #,  'quartile_3' #, 'percentile_95': #, 'largest_value': #}
    '''
    if not samples:
      return {"quartile_1": 0, "median": 0, "quartile_3": 0, "percentile_95": 0, "largest_value": 0}
    sorted_samples = sorted(samples)
    num_samples = len(sorted_samples)
    pos_quartile1 = (num_samples) / 4
    pos_quartile3 = (num_samples * 3)  / 4
    pos_95 = (num_samples * 95) / 100
    quartile1 = sorted_samples[pos_quartile1]
    median = sorted_samples[num_samples/2]
    quartile3 = sorted_samples[pos_quartile3]
    percentile_95 = sorted_samples[pos_95]
    largest_value = sorted_samples[-1]
    return {"quartile_1": quartile1,
            "median": median,
            "quartile_3": quartile3,
            "percentile_95": percentile_95,
            "largest_value": largest_value}

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
