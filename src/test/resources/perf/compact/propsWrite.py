#! /usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'bachmanm'

import matplotlib.pyplot as plt
import numpy as np

data = np.empty((6, 13, 22), int)
data[0, :, :] = np.loadtxt(open("twoPropsPlainDatabaseWriteCompact.txt", "rb"), delimiter=";", dtype=int,
                           usecols=(range(0, 22)))
data[1, :, :] = np.loadtxt(open("fourPropsPlainDatabaseWrite.txt", "rb"), delimiter=";", dtype=int,
                           usecols=(range(0, 22)))
data[2, :, :] = np.loadtxt(open("twoPropsFullRelcountWriteCompact-first1k.txt", "rb"), delimiter=";", dtype=int,
                           usecols=(range(0, 22)))
data[3, :, :] = np.loadtxt(open("twoPropsFullRelcountWriteCompact-second1k.txt", "rb"), delimiter=";", dtype=int,
                           usecols=(range(0, 22)))
data[4, :, :] = np.loadtxt(open("fourPropsFullRelcountWrite-first1k.txt", "rb"), delimiter=";", dtype=int,
                           usecols=(range(0, 22)))
data[5, :, :] = np.loadtxt(open("fourPropsFullRelcountWrite-second1k.txt", "rb"), delimiter=";", dtype=int,
                           usecols=(range(0, 22)))
# data[3, :, :] = np.loadtxt(open("twoPropsFullRelcountWriteCompact-last1k.txt", "rb"), delimiter=";", dtype=int, usecols=(range(0,22)))

means = np.mean(data[:, :, 3:], 2)

stddevs = np.std(data[:, :, 3:], 2)

xaxis = data[0, :, 1]

plt.plot(xaxis, means[0, :], c="purple", ls='-', linewidth=2.0)
plt.plot(xaxis, means[1, :], c="pink", ls='-', linewidth=2.0)
plt.plot(xaxis, means[2, :], c="blue", ls='-', linewidth=2.0)
plt.plot(xaxis, means[3, :], c="blue", ls='-.', linewidth=2.0)
plt.plot(xaxis, means[4, :], c="red", ls='-', linewidth=2.0)
plt.plot(xaxis, means[5, :], c="red", ls='-.', linewidth=2.0)
plt.xlabel('Number of Relationships per Transaction')
plt.ylabel('Time (microseconds)')
plt.title('Creating Relationships with Two Properties (Compaction)')
plt.legend(('Plain Database, 2 Props (A)',
            'Plain Database, 4 Props (B)',
            'Full Relcount, 2 Props, First 1k (C)',
            'Full Relcount, 2 Props, Second 1k (D)',
            'Full Relcount, 4 Props First 1k (E)',
            'Full Relcount, 4 Props Second 1k (F)',
           ), loc=3)
plt.yscale('log')
plt.xscale('log')
# plt.errorbar(xaxis, means[0], yerr=stddevs[0])
# plt.errorbar(xaxis, means[1], yerr=stddevs[1])
# plt.errorbar(xaxis, means[2], yerr=stddevs[2])
plt.show()

# for LaTeX:

twozero = (1.0 / data[2, :, 2:]) / (1.0 / data[0, :, 2:])
threezero = (1.0 / data[3, :, 2:]) / (1.0 / data[0, :, 2:])
fourone = (1.0 / data[4, :, 2:]) / (1.0 / data[1, :, 2:])
fiveone = (1.0 / data[5, :, 2:]) / (1.0 / data[1, :, 2:])

_20m = np.mean(twozero, 1)
_30m = np.mean(threezero, 1)
_41m = np.mean(fourone, 1)
_51m = np.mean(fiveone, 1)

_20s = np.std(twozero, 1)
_30s = np.std(threezero, 1)
_41s = np.std(fourone, 1)
_51s = np.std(fiveone, 1)

s = ' Rels / Tx & C & D & E & F \\\\ \\hline \\hline \n'

for i in range(0, 13):
    s = s + "{0:.0f}".format(xaxis[i]) + \
        ' & $' + "{0:.1f}\%".format(_20m[i] * 100) + ' \\pm ' "{0:.1f}\%".format(_20s[i] * 100) + '$' + \
        ' & $' + "{0:.1f}\%".format(_30m[i] * 100) + ' \\pm ' "{0:.1f}\%".format(_30s[i] * 100) + '$' + \
        ' & $' + "{0:.1f}\%".format(_41m[i] * 100) + ' \\pm ' "{0:.1f}\%".format(_41s[i] * 100) + '$' + \
        ' & $' + "{0:.1f}\%".format(_51m[i] * 100) + ' \\pm ' "{0:.1f}\%".format(_51s[i] * 100) + '$' + \
        ' \\\\ \\hline \n'

print s

s = ' Rels / Tx & A & B & C & D & E & F  \\\\ \\hline \\hline \n'

for i in range(0, 13):
    s = s + "{0:.0f}".format(xaxis[i]) + \
        ' & $' + "{0:.1f}".format(np.mean(data[0, i, 2:], 0) / 1000.0) + ' \\pm ' "{0:.1f}".format(
        np.std(data[0, i, 2:], 0) / 1000.0) + '$' + \
        ' & $' + "{0:.1f}".format(np.mean(data[1, i, 2:], 0) / 1000.0) + ' \\pm ' "{0:.1f}".format(
        np.std(data[1, i, 2:], 0) / 1000.0) + '$' + \
        ' & $' + "{0:.1f}".format(np.mean(data[2, i, 2:], 0) / 1000.0) + ' \\pm ' "{0:.1f}".format(
        np.std(data[2, i, 2:], 0) / 1000.0) + '$' + \
        ' & $' + "{0:.1f}".format(np.mean(data[3, i, 2:], 0) / 1000.0) + ' \\pm ' "{0:.1f}".format(
        np.std(data[3, i, 2:], 0) / 1000.0) + '$' + \
        ' & $' + "{0:.1f}".format(np.mean(data[4, i, 2:], 0) / 1000.0) + ' \\pm ' "{0:.1f}".format(
        np.std(data[4, i, 2:], 0) / 1000.0) + '$' + \
        ' & $' + "{0:.1f}".format(np.mean(data[5, i, 2:], 0) / 1000.0) + ' \\pm ' "{0:.1f}".format(
        np.std(data[5, i, 2:], 0) / 1000.0) + '$' + \
        ' \\\\ \\hline \n'

print s


