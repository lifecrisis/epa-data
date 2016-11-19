"""
Generate distinct partitions over which to average k-fold results.
"""


import random


def read_records(file_name):
    """ Return a list of the CSV records from file_name. """

    file_obj = open(file_name, 'r')
    record_list = file_obj.readlines()
    file_obj.close()
    return record_list

def write_records(file_name):
    """ Shuffle the global record list and write to "filename". """

    record_list = read_records(file_name)
    random.shuffle(record_list)
    out_file = open('partitions/' +\
                    file_name[file_name.rfind('/') + 1:].\
                    replace('.csv', '_partition.csv'), 'w')
    out_file.writelines(record_list)
    out_file.close()

def main():
    """ Application main. """

    csv_file_names = ['../data/clean/monthly_no2_1990-2015.csv',
                      '../data/clean/monthly_ozone_1990-2015.csv',
                      '../data/clean/monthly_pm25_1990-2015.csv']
    for file_name in csv_file_names:
        write_records(file_name)

if __name__ == "__main__":
    main()
