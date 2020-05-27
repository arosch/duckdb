import csv
import numpy as np
import numpy.random as nr
import scipy.stats as ss


def distribution(min_val, max_val, mean, std):
    scale = max_val - min_val
    location = min_val
    # Mean and standard deviation of the unscaled beta distribution
    unscaled_mean = (mean - min_val) / scale
    unscaled_var = (std / scale) ** 2
    # Computation of alpha and beta can be derived from mean and variance formulas
    t = unscaled_mean / (1 - unscaled_mean)
    beta = ((t / unscaled_var) - (t * t) - (2 * t) - 1) / ((t * t * t) + (3 * t * t) + (3 * t) + 1)
    alpha = beta * t
    # Not all parameters may produce a valid distribution
    if alpha <= 0 or beta <= 0:
        raise ValueError('Cannot create distribution for the given parameters.')
    # Make scaled beta distribution with computed parameters
    return ss.beta(alpha, beta, scale=scale, loc=location)

np.random.seed(100)

ntuples = [1000000, 100000000]
means = [5, 10, 20, 30, 40, 50, 60, 70, 80, 90]

min_val = 1.0
max_val = 100

for ntuple in ntuples:
    for mean in means:
        std = 7
        dist = distribution(min_val, max_val, mean, std)      
        size = int(ntuple//mean)
        case_lengths = dist.rvs(size=size).astype(int)
        print('min:', case_lengths.min(), 'max:', case_lengths.max())
        with open('data/data-' + str(ntuple) + '-' + str(mean) + '.csv', 'w') as csvfile:
            writer = csv.writer(csvfile, delimiter=' ')
            caseid = 0
            for case_length in np.nditer(case_lengths):
                case = [caseid for i in range(case_length)]
                activity = [0] + nr.randint(0, 10, case_length)
                timestamp = range(case_length)
                for c, a, t in zip(case, activity, timestamp):
                    writer.writerow([c] + [a] + [t])
                caseid+= 1
