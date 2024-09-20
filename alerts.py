import random
from random import normalvariate

def generate_data_stream(mu=125.0, sigma=20.0):
    """
    Generator function to simulate a stream of stock prices.
    Parameters:
    - mu (float): The mean of the distribution.
    - sigma (float): The standard deviation of the distribution.
    Yields:
    - float: A simulated stock price.
    """
    while True:
        yield normalvariate(mu, sigma)

class EBPPS:
    def __init__(self, k):
        self.k = k  # Number of samples to maintain
        self.reservoir = [None] * k
        self.n = 0  # Number of elements seen so far

    def update(self, x):
        self.n += 1
        if self.n <= self.k:
            self.reservoir[self.n - 1] = x
        else:
            p = self.k / self.n
            if random.random() < p:
                self.reservoir[random.randint(0, self.k - 1)] = x

    def get_samples(self):
        return [x for x in self.reservoir if x is not None]
    

def compute_moments(samples):
    n = len(samples)
    if n == 0:
        return None, None, None, None  # Handle case where no samples are available
    mean = sum(samples) / n
    variance = sum((x - mean) ** 2 for x in samples) / n
    if variance == 0:
        skewness = 0  # Skewness is zero if all values are the same
        kurtosis = 0  # Kurtosis is zero if all values are the same
    else:
        skewness = (sum((x - mean) ** 3 for x in samples) / n) / (variance ** 1.5)
        kurtosis = (sum((x - mean) ** 4 for x in samples) / n) / (variance ** 2)
    return mean, variance, skewness, kurtosis



def check_alerts(mean, variance, skewness, kurtosis, thresholds):
    alerts = []
    if abs(mean - thresholds['mean']) > thresholds['mean_delta']:
        alerts.append("Mean deviation alert")
    if abs(variance - thresholds['variance']) > thresholds['variance_delta']:
        alerts.append("Variance spike alert")
    if abs(skewness - thresholds['skewness']) > thresholds['skewness_delta']:
        alerts.append("Skewness change alert")
    if abs(kurtosis - thresholds['kurtosis']) > thresholds['kurtosis_delta']:
        alerts.append("Kurtosis spike alert")
    return alerts

# Initialize variables and thresholds
k = 5
thresholds = {'mean': 120, 'mean_delta': 10, 'variance': 90, 'variance_delta': 40, 
              'skewness': 0, 'skewness_delta': 0.5, 'kurtosis': 3, 'kurtosis_delta': 1}

ebpps = EBPPS(k)
stream = generate_data_stream()


for _ in range(10000):  # adjust this for longer streams
    x = next(stream)
    ebpps.update(x)
    if _ % 500 == 0:  # Calculate moments 
        samples = ebpps.get_samples()
        results = compute_moments(samples)
        if None not in results:
            mean, variance, skewness, kurtosis = results
            alerts = check_alerts(mean, variance, skewness, kurtosis, thresholds)
            if alerts:
                print(f"Alerts at step {_}: {alerts}")
        else:
            print("Insufficient data for moment calculation.")
