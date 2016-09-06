#! /bin/bash
#
# This script downloads and unpacks data required for our analysis prior
# to visiting the CDC. The site hosting the data can be found at the
# following URI...
#
# http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/download_files.html
#


# URIs at which data are located.
OZONE_URI="http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/"
PM_25_URI="http://aqsdr1.epa.gov/aqsweb/aqstmp/airdata/"


# Build the filename of the required .ZIP file of ozone measurments.
# Usage: ozone_file year
ozone_file() {
    local year

    year="$1"
    echo "daily_44201_$year.zip"
}

# Build the filename of the required .ZIP file of pm2.5 measurments.
# Usage: pm_25_file year
pm_25_file() {
    local year

    year="$1"
    echo "daily_88101_$year.zip"
}


# Switch to the project root directory.
cd "${BASH_SOURCE%/*}/.." || exit

# Prepare "data/" directory for download of EPA data.
if [ -d data/ ]; then
    rm -rf data/*
else
    mkdir data/
fi
cd data/

# Download all required .ZIP files.
for year in {1990..2015}; do
    wget --directory-prefix='ozone_raw_data/' \
         "$OZONE_URI$(ozone_file $year)"
    wget --directory-prefix='pm_25_raw_data/' \
         "$PM_25_URI$(pm_25_file $year)"
done

# Unzip all .ZIP archives.
# TODO(jf): Sort out the problems using `unzip` with wildcards.
pushd ozone_raw_data/; unzip \*.zip; popd
pushd pm_25_raw_data/; unzip \*.zip; popd

# Remove .ZIP archives, leaving only the required .CSV files.
rm ozone_raw_data/*.zip pm_25_raw_data/*.zip
