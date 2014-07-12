__author__ = 'bachmanm'

import matplotlib.pyplot as plt
import numpy as np

filename = "countRelationships.txt"
noParams = 5

resultsAsArray = np.loadtxt(open(filename, "rb"), delimiter=";", dtype=str, skiprows=3)

means = np.mean(resultsAsArray[:, noParams:].astype(int), 1)
stddevs = np.std(resultsAsArray[:, noParams:].astype(int), 1)


def f(arr, value):
    return arr[np.where(arr == value)[0:1]]


def plot(toPlot, color, ls):
    xaxis = toPlot[:, 2].astype(int)
    data = toPlot[:, noParams:].astype(int)
    plt.plot(xaxis, np.mean(data, 1), c=color, ls=ls, linewidth=2.0)
    # plt.errorbar(xaxis, np.mean(data,1), yerr=(np.std(data, 1)))


storage = "SINGLE_PROP"
# storage = "MULTI_PROP"

props = "NO_PROPS"
# props = "TWO_PROPS"

plot(f(f(f(f(resultsAsArray, storage), props), "nocache"), "NO_FRAMEWORK"), "purple", ":")
# plot(f(f(f(f(resultsAsArray, storage), props), "nocache"), "NAIVE"), "purple","-.")
plot(f(f(f(f(resultsAsArray, storage), props), "nocache"), "NAIVE_OPTIMIZED"), "purple","-.")
plot(f(f(f(f(resultsAsArray, storage), props), "nocache"), "CACHED"), "purple", "-")
plot(f(f(f(f(resultsAsArray, storage), props), "lowcache"), "NO_FRAMEWORK"), "green", ":")
# plot(f(f(f(f(resultsAsArray, storage), props), "lowcache"), "NAIVE"), "green","-.")
plot(f(f(f(f(resultsAsArray, storage), props), "lowcache"), "NAIVE_OPTIMIZED"), "green","-.")
plot(f(f(f(f(resultsAsArray, storage), props), "lowcache"), "CACHED"), "green", "-")
plot(f(f(f(f(resultsAsArray, storage), props), "highcache"), "NO_FRAMEWORK"), "blue", ":")
# plot(f(f(f(f(resultsAsArray, storage), props), "highcache"), "NAIVE"), "blue","-.")
plot(f(f(f(f(resultsAsArray, storage), props), "highcache"), "NAIVE_OPTIMIZED"), "blue","-.")
plot(f(f(f(f(resultsAsArray, storage), props), "highcache"), "CACHED"), "blue", "-")

plt.xlabel('Relationships per Node')
plt.ylabel('Time (microseconds)')
plt.title('Counting Relationships for 10 Nodes (Two Properties per Relationship)')
plt.legend(("Plain Neo4j (disk)", "RelCount Module Naive (disk)", "RelCount Module Cached (disk)",
            "Plain Neo4j (low level cache)", "RelCount Module Naive (low level cache)", "RelCount Module Cached (low level cache)",
            "Plain Neo4j (high level cache)","RelCount Module Naive (high level cache)","RelCount Module Cached (high level cache)"), loc=0)
plt.yscale('log')
plt.xscale('log')
plt.show()
