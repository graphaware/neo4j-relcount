#! /usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'bachmanm'

import matplotlib.pyplot as plt
import numpy as np

data = np.empty((4, 4, 12), int)
data[0, :, :] = np.loadtxt(open("noPropsPlainDatabaseWrite.txt", "rb"), delimiter=";", dtype=int)
data[1, :, :] = np.loadtxt(open("noPropsEmptyFrameworkWrite.txt", "rb"), delimiter=";", dtype=int)
data[2, :, :] = np.loadtxt(open("noPropsSimpleRelcountWrite.txt", "rb"), delimiter=";", dtype=int)
data[3, :, :] = np.loadtxt(open("noPropsFullRelcountWrite.txt", "rb"), delimiter=";", dtype=int)

means = np.mean(data[:, :, 2:], 2)

stddevs = np.std(data[:, :, 2:], 2)

xaxis = [1, 10, 100, 1000]

plt.plot(xaxis, means[0], c="purple", linewidth=2.0)
plt.plot(xaxis, means[1], c="red", linewidth=2.0)
plt.plot(xaxis, means[2], c="green", linewidth=2.0)
plt.plot(xaxis, means[3], c="orange", linewidth=2.0)
plt.xlabel('Number of Relationships per Transaction')
plt.ylabel('Time (ms)')
plt.title('Creating 100,000 Relationships with No Properties')
plt.legend(('Plain Database', 'Empty Framework', 'Simple Relcount', 'Full Relcount'), loc=3)
plt.yscale('log')
plt.xscale('log')
# plt.errorbar(xaxis, means[0], yerr=stddevs[0])
# plt.errorbar(xaxis, means[1], yerr=stddevs[1])
# plt.errorbar(xaxis, means[2], yerr=stddevs[2])
# plt.errorbar(xaxis, means[3], yerr=stddevs[3])
plt.show()

# for LaTeX:

none = (1.0 / data[0, :, 2:]) / (1.0 / data[0, :, 2:])
empty = (1.0 / data[1, :, 2:]) / (1.0 / data[0, :, 2:])
simple = (1.0 / data[2, :, 2:]) / (1.0 / data[0, :, 2:])
full = (1.0 / data[3, :, 2:]) / (1.0 / data[0, :, 2:])

emptymean = np.mean(empty, 1)
simplemean = np.mean(simple, 1)
fullmean = np.mean(full, 1)

emptydev = np.std(empty, 1)
simpledev = np.std(simple, 1)
fulldev = np.std(full, 1)

s = ' & ' + ' & '.join(str(x) for x in [1, 10, 100, 1000]) + ' \\\\ \\hline \n'
s = s + ' No Framework & $100\%$ & $100\%$ & $100\%$ & $100\%$  \\\\ \\hline \n'

s = s + 'Empty Framework '
for i in range(0, 4):
    s = s + ' & $' + "{0:.0f}\%".format(emptymean[i] * 100) + ' \\pm ' "{0:.0f}\%".format(emptydev[i] * 100) + '$'
s = s + ' \\\\ \\hline \n'

s = s + 'Simple Relcount '
for i in range(0, 4):
    s = s + ' & $' + "{0:.0f}\%".format(simplemean[i] * 100) + ' \\pm ' "{0:.0f}\%".format(simpledev[i] * 100) + '$'
s = s + ' \\\\ \\hline \n'

s = s + 'Full Relcount '
for i in range(0, 4):
    s = s + ' & $' + "{0:.0f}\%".format(fullmean[i] * 100) + ' \\pm ' "{0:.0f}\%".format(fulldev[i] * 100) + '$'
s = s + ' \\\\ \\hline \n'

print s