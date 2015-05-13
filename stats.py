import datetime

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
  def __init__(self, counter_data, current_epoch_sec):
    '''Assumes that the counter_data is the COUNTER_JSON object per the protocol definition'''
    self.counter_data = counter_data
    self.current_epoch_sec = current_epoch_sec

  def last_minute_count(self):
    # TODO: Make this the prior minute to the current one; only change when SamplerStats has
    # similar behavior
    curr_time_dt = utils.sec_since_epoch_to_datetime(self.current_epoch_sec)
    last_min_since_epoch = utils.epoch_sec_to_minutes_since_epoch(self.counter_data["latest_time_sec"])
    curr_min_since_epoch = utils.epoch_sec_to_minutes_since_epoch(self.current_epoch_sec)


    # It actually wouldn't be that hard to handle the case where we have data beyond the
    # current_time, but let's not bother
    # Obviously, if the curr_min is ahead of the last_min_since_epoch, we don't have data
    if curr_min_since_epoch != last_min_since_epoch:
      return 0

    curr_min_of_hour = curr_time_dt.minute
    return self.counter_data["min_counters"][curr_min_of_hour]

  def last_hour_count(self):
    last_min_since_epoch = utils.epoch_sec_to_minutes_since_epoch(self.counter_data["latest_time_sec"])
    curr_min_since_epoch = utils.epoch_sec_to_minutes_since_epoch(self.current_epoch_sec)

    # If the current time is more than 1 hour aheads of the data return 0
    # OR
    # We somehow have data ahead of the current_time (we won't try to resolve that)
    difference_btw_curr_and_last = curr_min_since_epoch - last_min_since_epoch
    if difference_btw_curr_and_last >= 60 or difference_btw_curr_and_last < 0:
      return 0

    # Example:
    # curr_min_since_epoch = 620  / curr_min_since_epoch = 20
    # last_min_since_epoch = 615  / last_min_of_hour_with_data = 15
    # The array therefore looks something like
    # counter_data[14] = Data from 614
    # counter_data[15] = Data from 615 (last recorded value, 0 minutes old)
    # counter_data[16] = Data from 556 (64 minutes old)
    # counter_data[20] = Data from 560 (60 minutes old)
    # Thus we want values starting from position 15 going backward through position 21

    last_min_of_hour_with_data = last_min_since_epoch % 60
    num_mins_with_data_in_last_hour = 60 - difference_btw_curr_and_last
    filtered_mins = self._filter_counters_data(last_min_of_hour_with_data,
                                               num_mins_with_data_in_last_hour,
                                               self.counter_data["min_counters"]) 
    return sum(filtered_mins)

  def _filter_counters_data(self, start_minute, num_minutes_to_go_back, min_counters):
    '''Start at the provided start_minute and provide the minutes for the next 
       num_minutes_to_go_back minutes, throwing the rest out'''
    filtered_mins = []
    for i in range(0, num_minutes_to_go_back):
      cur_min_pos = (start_minute - i) % 60
      filtered_mins.append(min_counters[cur_min_pos])
    return filtered_mins

  def all_time_count(self):
    return self.counter_data["all_time_count"]
