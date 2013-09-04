#! /usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'bachmanm'

import matplotlib.pyplot as plt
import fileinput
import numpy as np
import csv
import array as a

data = np.empty((3, 26, 101), int)
data[0, :, :] = np.loadtxt(open("twoPropsReading-disk.txt", "rb"), delimiter=";", dtype=int, usecols=(range(1, 102)))
data[1, :, :] = np.loadtxt(open("twoPropsReading-nocache.txt", "rb"), delimiter=";", dtype=int, usecols=(range(1, 102)))
data[2, :, :] = np.loadtxt(open("twoPropsReading-strong-cache.txt", "rb"), delimiter=";", dtype=int,
                           usecols=(range(1, 102)))

means = np.mean(data[:, :, 1:], 2)

stddevs = np.std(data[:, :, 1:], 2)

xaxis = 2 * data[0, 0:13, 0] / 100
print xaxis

plt.plot(xaxis, means[0, 0:13], c="purple", ls=':', linewidth=2.0)
plt.plot(xaxis, means[1, 0:13], c="purple", ls='-.', linewidth=2.0)
plt.plot(xaxis, means[2, 0:13], c="purple", ls='-', linewidth=2.0)
plt.plot(xaxis, means[0, 13:26], c="blue", ls=':', linewidth=2.0)
plt.plot(xaxis, means[1, 13:26], c="blue", ls='-.', linewidth=2.0)
plt.plot(xaxis, means[2, 13:26], c="blue", ls='-', linewidth=2.0)
plt.xlabel('Average Vertex Degree')
plt.ylabel('Time (microseconds)')
plt.title('Computing Degrees of 10 Random Vertices, Two Rel Properties')
plt.legend(('Plain Database (No Cache)',
            'Plain Database (Low Level)',
            'Plain Database (High Level)',
            'Full Relcount (No Cache)',
            'Full Relcount (Low Level)',
            'Full Relcount (High Level)'), loc=2)
plt.yscale('log')
plt.xscale('log')
# plt.errorbar(xaxis, means[0:4], yerr=stddevs[0:4])
# plt.errorbar(xaxis, means[4:8], yerr=stddevs[4:8])
# plt.errorbar(xaxis, means[2], yerr=stddevs[2])
# plt.errorbar(xaxis, means[3], yerr=stddevs[3])
plt.show()

disk = (1.0 / data[0, 13:26, 1:]) / (1.0 / data[0, 0:13, 1:])
low = (1.0 / data[1, 13:26, 1:]) / (1.0 / data[1, 0:13, 1:])
high = (1.0 / data[2, 13:26, 1:]) / (1.0 / data[2, 0:13, 1:])

print disk

diskmean = np.mean(disk, 1)
lowmean = np.mean(low, 1)
highmean = np.mean(high, 1)

diskdev = np.std(disk, 1)
lowedev = np.std(low, 1)
highdev = np.std(high, 1)

s = ' Avg Degree & No Cache & Low Level & High Level \\\\ \\hline \\hline \n'

for i in range(0, 13):
    s = s + "{0:.0f}".format(xaxis[i]) + \
        ' & $' + "{0:.2f}".format(diskmean[i]) + ' \\pm ' "{0:.2f}".format(diskdev[i]) + '$' + \
        ' & $' + "{0:.2f}".format(lowmean[i]) + ' \\pm ' "{0:.2f}".format(lowedev[i]) + '$' + \
        ' & $' + "{0:.2f}".format(highmean[i]) + ' \\pm ' "{0:.2f}".format(highdev[i]) + '$' + \
        ' \\\\ \\hline \n'

print s
