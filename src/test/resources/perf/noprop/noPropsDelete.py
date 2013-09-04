#! /usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'bachmanm'

import matplotlib.pyplot as plt
import numpy as np

data = np.empty((3, 13, 22), int)
data[0, :, :] = np.loadtxt(open("noPropsPlainDatabaseDelete.txt", "rb"), delimiter=";", dtype=int)
data[1, :, :] = np.loadtxt(open("noPropsSimpleRelcountDelete.txt", "rb"), delimiter=";", dtype=int)
data[2, :, :] = np.loadtxt(open("noPropsFullRelcountDelete.txt", "rb"), delimiter=";", dtype=int)

means = np.mean(data[:, :, 2:], 2)

stddevs = np.std(data[:, :, 2:], 2)

xaxis = data[0, :, 1]

plt.plot(xaxis, means[0], c="purple", linewidth=2.0)
plt.plot(xaxis, means[1], c="green", linewidth=2.0)
plt.plot(xaxis, means[2], c="orange", linewidth=2.0)
plt.xlabel('Number of Relationships per Transaction')
plt.ylabel('Time (ms)')
plt.title('Deleting 10,000 Relationships with No Properties')
plt.legend(('Plain Database', 'Simple Relcount', 'Full Relcount'), loc=3)
plt.yscale('log')
plt.xscale('log')
plt.errorbar(xaxis, means[0], yerr=stddevs[0])
plt.errorbar(xaxis, means[1], yerr=stddevs[1])
plt.errorbar(xaxis, means[2], yerr=stddevs[2])
# plt.show()

# for LaTeX:

none = (1.0 / data[0, :, 2:]) / (1.0 / data[0, :, 2:])
simple = (1.0 / data[1, :, 2:]) / (1.0 / data[0, :, 2:])
full = (1.0 / data[2, :, 2:]) / (1.0 / data[0, :, 2:])

simplemean = np.mean(simple, 1)
fullmean = np.mean(full, 1)

simpledev = np.std(simple, 1)
fulldev = np.std(full, 1)

s = ' Rels / Tx & Simple Relcount & Full Relcount \\\\ \\hline \\hline \n'

for i in range(0, 13):
    s = s + "{0:.0f}".format(xaxis[i]) + \
        ' & $' + "{0:.0f}\%".format(simplemean[i] * 100) + ' \\pm ' "{0:.0f}\%".format(simpledev[i] * 100) + '$' + \
        ' & $' + "{0:.0f}\%".format(fullmean[i] * 100) + ' \\pm ' "{0:.0f}\%".format(fulldev[i] * 100) + '$' + \
        ' \\\\ \\hline \n'

print s

s = ' Rels / Tx & No Framework & Simple Relcount & Full Relcount \\\\ \\hline \\hline \n'

for i in range(0, 13):
    s = s + "{0:.0f}".format(xaxis[i]) + \
        ' & $' + "{0:.0f}".format(np.mean(data[0, i, 2:], 0) / 1000.0) + ' \\pm ' "{0:.0f}".format(
        np.std(data[0, i, 2:], 0) / 1000.0) + '$' + \
        ' & $' + "{0:.0f}".format(np.mean(data[1, i, 2:], 0) / 1000.0) + ' \\pm ' "{0:.0f}".format(
        np.std(data[1, i, 2:], 0) / 1000.0) + '$' + \
        ' & $' + "{0:.0f}".format(np.mean(data[2, i, 2:], 0) / 1000.0) + ' \\pm ' "{0:.0f}".format(
        np.std(data[2, i, 2:], 0) / 1000.0) + '$' + \
        ' \\\\ \\hline \n'

print s
