import unittest

import stats

class SamplerStatsTestCase(unittest.TestCase):
  def setUp(self):
    pass
  
  def createFakeData(self, all_time_duration_sec=3600*24*3, all_time_samples=1000, all_time_events=5000):
    latest_time_sec = 1400000000
    minute_samples = self.createFakeSamples(latest_time_sec, latest_time_sec - 60, 100, 2000)
    alltime_samples = self.createFakeSamples(latest_time_sec,
                                             latest_time_sec - all_time_duration_sec,
                                             all_time_samples,
                                             all_time_events)
    return {"latest_time_sec": latest_time_sec,
            "last_minute_samples": minute_samples,
            "all_time_samples": alltime_samples}

  def createFakeSamples(self, start_time, end_time, num_samples, num_events=None):
    num_events = num_events if num_events is not None else num_samples
    step = (end_time - start_time) / float(num_samples)
    sample_times_sec = [start_time + int(x*step) for x in range(0, num_samples)]
    return {"sample_values": range(0, num_samples),
            "sample_times_sec": sample_times_sec,
            "samples_size": num_samples,
            "num_events": num_events}


  def test_last_minute_stats(self):
    s = stats.SamplerStats(self.createFakeData())
    min_stats = s.last_minute_stats()
    self.assertEquals(25, min_stats["quartile_1"])
    self.assertEquals(50, min_stats["median"])
    self.assertEquals(75, min_stats["quartile_3"])
    self.assertEquals(95, min_stats["percentile_95"])
    self.assertEquals(99, min_stats["largest_value"])
    self.assertEquals(2000, min_stats["count"])
 
  def test_all_time_stats(self):  
    s = stats.SamplerStats(self.createFakeData())
    at_stats= s.all_time_stats()
    self.assertEquals(250, at_stats["quartile_1"])
    self.assertEquals(500, at_stats["median"])
    self.assertEquals(750, at_stats["quartile_3"])
    self.assertEquals(950, at_stats["percentile_95"])
    self.assertEquals(999, at_stats["largest_value"])
    self.assertEquals(5000, at_stats["count"])

  def test_hour_stats(self):
    s = stats.SamplerStats(self.createFakeData(3600*3, 600, 6000))
    hr_stats = s.last_hour_stats()
    self.assertEquals(50, hr_stats["quartile_1"])
    self.assertEquals(100, hr_stats["median"])
    self.assertEquals(150, hr_stats["quartile_3"])
    self.assertEquals(190, hr_stats["percentile_95"])
    self.assertEquals(199, hr_stats["largest_value"])
    self.assertEquals(2000, hr_stats["count"])

if __name__ == "__main__":
  unittest.main()
