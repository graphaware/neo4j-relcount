#! /usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'bachmanm'

import matplotlib.pyplot as plt
import fileinput
import numpy as np
import csv
import array as a

data = np.empty((8, 12), int)
data = np.loadtxt(open("noPropsReading.txt", "rb"), delimiter=";", dtype=int)

means = np.mean(data[:, 2:], 1)

stddevs = np.std(data[:, 2:], 1)

xaxis = [10, 100, 1000, 10000]

plt.plot(xaxis, means[4:8], c="purple", linewidth=2.0)
plt.plot(xaxis, means[0:4], c="green", linewidth=2.0)
plt.xlabel('Average Vertex Degree')
plt.ylabel('Time (ms)')
plt.title('Computing Degrees of 1 Mil. Random Vertices, No Rel Properties')
plt.legend(('Plain Database', 'Simple Relcount'), loc=2)
plt.yscale('log')
plt.xscale('log')
# plt.errorbar(xaxis, means[0:4], yerr=stddevs[0:4])
# plt.errorbar(xaxis, means[4:8], yerr=stddevs[4:8])
# plt.errorbar(xaxis, means[2], yerr=stddevs[2])
# plt.errorbar(xaxis, means[3], yerr=stddevs[3])
plt.show()

# for LaTeX:

simple = (1.0 / data[0:4, 2:]) / (1.0 / data[4:8, 2:])

simplemean = np.mean(simple, 1)
simpledev = np.std(simple, 1)

s = ' & ' + ' & '.join(str(x) for x in [10, 100, 1000, 10000]) + ' \\\\ \\hline \n'

s = s + 'Speedup '
for i in range(0, 4):
    s = s + ' & $' + "{0:.2f}".format(simplemean[i]) + ' \\pm ' "{0:.2f}".format(simpledev[i]) + '$'
s = s + ' \\\\ \\hline \n'

print s
