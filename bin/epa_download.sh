#! /bin/bash
#
# This script downloads and unpacks data required for our analysis prior
# to visiting the CDC. The site hosting the data can be found at the
# following URI...
#
# http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/download_files.html
#


# URI at which the data are located.
DATA_URI="http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/"


# Build the filename of the required .ZIP file of no2 measurments.
# Usage: no2_file year
no2_file() {
    local year

    year="$1"
    echo "daily_42602_$year.zip"
}

# Build the filename of the required .ZIP file of ozone measurments.
# Usage: ozone_file year
ozone_file() {
    local year

    year="$1"
    echo "daily_44201_$year.zip"
}

# Build the filename of the required .ZIP file of pm25 measurments.
# Usage: pm25_file year
pm25_file() {
    local year

    year="$1"
    echo "daily_88101_$year.zip"
}


# Switch to the project root directory.
cd "${BASH_SOURCE%/*}/.." || exit

# Test that current working dir is, in fact, the project root.
if [ "$(basename $PWD)" != "epa-data" ]; then
    echo "Working directory is $PWD... expected 'epa-data'."
    exit 1
fi

# Prepare "data/" directory for download of EPA data.
if [ -d data/ ]; then
    rm -rf data/*
else
    mkdir data/
fi
cd data/

# Download all required .ZIP files.
for year in {1990..2015}; do
    wget --directory-prefix='no2_raw_data/'   "$DATA_URI$(no2_file $year)"
    wget --directory-prefix='ozone_raw_data/' "$DATA_URI$(ozone_file $year)"
    wget --directory-prefix='pm25_raw_data/'  "$DATA_URI$(pm25_file $year)"
done

# Unzip all .ZIP archives.
# TODO(jf): Sort out the problems using `unzip` with wildcards.
pushd no2_raw_data/; unzip \*.zip; popd
pushd ozone_raw_data/; unzip \*.zip; popd
pushd pm25_raw_data/; unzip \*.zip; popd

# Remove .ZIP archives, leaving only the required .CSV files.
rm no2_raw_data/*.zip ozone_raw_data/*.zip pm25_raw_data/*.zip
