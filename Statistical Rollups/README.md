# Statistical Rollups
This example demonstrates how to calculate some very simple statistics (eg, mean and standard deviation) for climate data stored in netCDF files in embarassingly parallel fashion. Here we take raw daily data and then accumulate the results into a monthly format, and from there aggregate again to return the total statistics.

## Pre-requisites
### Python
- pyspark 2.x
- clim (to be uploaded to the SciSpark repo at a later date)
- numpy
- netCDF4

We also recommend using [Mesos](http://mesos.apache.org/documentation/latest/) for your cluster manager if possible.

### Data
This benchmark has been tested with daily precipitation data from [TRMM](https://pmm.nasa.gov/data-access/downloads/trmm) (3B42) and [PRISM](prism.oregonstate.edu). The latter is available only in BIL or ASCII format by default, so you will need to convert them manually to netCDF4. We recommend using [GDAL](http://www.gdal.org/) to perform the conversion, and [nco](http://nco.sourceforge.net/) to rename the variables since the names provided by GDAL are not useful. If you have the [conda](https://conda.io/docs/) package manager installed, both can be obtained using

```
conda config --add channels conda-forge 
conda install gdal nco
```
We have provided a convenience script for downloading and converting all the PRISM data files:

```
chmod +x download_PRISM.sh
./download_PRISM.sh
```

If you are not using PRISM data, you will also need to edit `stats.py` to ensure that the filename matching pattern is correct. 

## Running the Benchmark
Use `spark-submit` as follows:

```
spark-submit --master <HOST:PORT> --total-executor-cores <NUMBER OF CORES> --conf spark.driver.maxResultSize=15g --conf spark.driver.memory=20g stats.py /path/to/data
```

Where `spark.driver.memory` and `spark.driver.maxResult` will depend on the size of the dataset you are using. The numbers in this example should work with the 4km PRISM dataset.
