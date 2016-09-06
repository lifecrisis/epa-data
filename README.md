# epa-data
This project performs a complicated cleansing process on pollution data
downloaded from the EPA.

## Purpose
Researchers at Augusta University have asked to use output from the learning
software built in [lifecrisis/aeolus](https://github.com/lifecrisis/aeolus).
This module downloads and cleans the data required for the analysis that
supports their work in linking pollution with cases of childhood asthma.

## Usage
This package is intended to be used in the following way:
1. First, run the `epa_download.sh` script in the `bin/` directory. This
will create the `data/` directory with two raw data subdirectories.
2. Next, invoke `epa_clean.sh` in the `bin/` directory. This will perform
the cleaning process and result in two new files being generated:
  * `data/epa_ozone.csv`
  * `data/epa_pm_25.csv`  
The structure of the files output by our Python scripts is suitable for use
with our learning algorithms.

## See Also
* https://github.com/lifecrisis/aeolus
* http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/download_files.html
