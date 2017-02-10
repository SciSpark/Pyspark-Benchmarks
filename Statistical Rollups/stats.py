# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from pyspark import SparkContext, SparkConf

import sys, os, re, time, glob
import timeit
import numpy as np
from netCDF4 import Dataset, default_fillvals

from clim.variables import getVariables, close
from clim.split import splitByMonth
from clim.cache import retrieveFile

def accumulate(urls, variable, accumulators=['count', 'mean', 'M2', 'min', 'max'], cachePath='~/cache'):
    '''Accumulate data into statistics accumulators like count, sum, sumsq, min, max, M3, M4, etc.'''
    keys, urls = urls
    accum = {}
    for i, url in enumerate(urls):
        try:
            path = retrieveFile(url, cachePath)
        except:
            print >>sys.stderr, 'accumulate: Error, continuing without file %s' % url
            continue

        try:
            print >>sys.stderr, 'Reading %s ...' % path
            var, fh = getVariables(path, [variable], arrayOnly=True, set_auto_mask=True)   # return dict of variable objects by name
            v = var[variable]   # could be masked array
            if v.shape[0] == 1: v = v[0]    # throw away trivial time dimension for CF-style files
            close(fh)
        except:
            print >>sys.stderr, 'accumulate: Error, cannot read variable %s from file %s' % (variable, path)
            continue

        if i == 0:
            for k in accumulators:
                if k == 'min':     accum[k] = default_fillvals['f8'] * np.ones(v.shape, dtype=np.float64)
                elif k == 'max':   accum[k] = -default_fillvals['f8'] * np.ones(v.shape, dtype=np.float64)
                elif k == 'count': accum[k] = np.zeros(v.shape, dtype=np.int64)
                else:
                    accum[k] = np.zeros(v.shape, dtype=np.float64)

        if np.ma.isMaskedArray(v):
            if 'count' in accumulators:
                accum['count'] += ~v.mask
            if 'min' in accumulators:
                accum['min'] = np.ma.minimum(accum['min'], v)
            if 'max' in accumulators:
                accum['max'] = np.ma.maximum(accum['max'], v)

            v = np.ma.filled(v, 0.)
    	else:
            if 'count' in accumulators:
                accum['count'] += 1
            if 'min' in accumulators:
                accum['min'] = np.minimum(accum['min'], v)
            if 'max' in accumulators:
                accum['max'] = np.maximum(accum['max'], v)

        if 'mean' in accumulators:
            n = accum['count']
            delta = v - accum['mean']     # subtract running mean from new values, eliminate roundoff errors
            delta_n = delta / n
            accum['mean'] += delta_n
        if 'M2' in accumulators:
            term = delta * delta_n * (n-1)
            accum['M2'] += term
    return (keys, accum)


def combine(a, b, accumulators=['count', 'mean', 'M2', 'min', 'max']):
    '''Combine accumulators by summing.'''
    keys, a = a
    b = b[1]
    if 'mean' in accumulators:
        ntotal = a['count'] + b['count']
        a['mean'] = (a['mean'] * a['count'] + b['mean'] * b['count']) / ntotal
    if 'min' in accumulators:
        if np.ma.isMaskedArray(a):
            a['min'] = np.ma.minimum(a['min'], b['min'])
        else:
            a['min'] = np.minimum(a['min'], b['min'])
    if 'max' in accumulators:
        if np.ma.isMaskedArray(a):
            a['max'] = np.ma.maximum(a['max'], b['max'])
        else:
            a['max'] = np.maximum(a['max'], b['max'])
    for k in a.keys():
        if k != 'mean' and k != 'min' and k != 'max':
            a[k] += b[k]      # just sum count and other moments
    return (('total',), a)


def statsFromAccumulators(accum):
    '''Compute final statistics from accumulators.'''
    keys, accum = accum
    
    # Mask all of the accumulator arrays
    accum['count'] = np.ma.masked_equal(accum['count'], 0, copy=False)
    mask = accum['count'].mask
    for k in accum:
        if k != 'count':
	    accum[k] = np.ma.array(accum[k], copy=False, mask=mask)

    # Compute stats (masked)
    stats = {}
    if 'count' in accum:
        stats['count'] = accum['count']
    if 'min' in accum:
        stats['min'] = accum['min']
    if 'max' in accum:
        stats['max'] = accum['max']
    if 'mean' in accum:
        stats['mean'] = accum['mean']
    if 'M2' in accum:
        stats['stddev'] = np.sqrt(accum['M2'] / (accum['count'] - 1))
    return (keys, stats)

    
def stats():
    '''Main spark program for the benchmark'''
    variable = 'pcp'
    ncores = int(sc.getConf().get('spark.cores.max'))
    urlsRDD = sc.parallelize(urlsByKey, numSlices=ncores*2)
    merged = urlsRDD.map(lambda urls: accumulate(urls, variable)).reduce(combine)
    stats = statsFromAccumulators(merged)

conf = SparkConf()
conf.set('spark.executor.cores', '5') # Cores allocated to each executor
conf.set('spark.mesos.coarse', 'true') # Needed if your cluster manager is mesos
conf.set('spark.executor.memory', '10G') # Memory to allocate to each executor
sc = SparkContext(appName='Stats Rollup Benchmark', conf=conf)

# Directory where netCDF data files are stored
directory = sys.argv[0]
urlList = glob.glob(os.path.join(directory, '*.nc'))

# This chunks the dataset by inidividual month (eg, January 1981) for the
# purpose of making a pair RDD.
urlsByKey = splitByMonth(urlList, {'get': ('year', 'month'), 'regex': re.compile(r'\/PRISM_ppt_stable_4kmD2_(....)(..).._bil.nc')})

# Run the benchmark 3 times to ensure that the cache is warmed up. For this reason,
#we recommend using the minimum (eg, best) time for benchmarking purposes.
print(timeit.repeat('stats()', number=1, repeat=3, setup="from __main__ import stats"))
