import random
from random import normalvariate

def generate_data_stream(mu=100.0, sigma=10.0):
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

# Example usage
k = 5
ebpps = EBPPS(k)
# stream = generate_data_stream()
stream = generate_data_stream(mu=100.0, sigma=10.0)

for _ in range(1000):
    price = next(stream)
    ebpps.update(price)
    print(price)

# for _ in range(1000):
#     x = next(stream)
#     ebpps.update(x)

samples = ebpps.get_samples()
print(f"Reservoir samples: {samples}")