"""
This module prints a summary report for the accompanying results files.
"""


import glob


def print_record(record):
    """ Format and print a CSV record. """

    fields = record.split(',')
    print "    Folds : %s" % fields[0]
    print "    Neighbors : %s" % fields[1]
    print "    Power : %s" % fields[2]
    print "    Time Scale : %s" % fields[3]
    print "    Alpha : %s" % fields[4]
    print "    Bags : %s" % fields[5]
    print "    MARE : %s" % fields[6]
    print "    RMSPE : %s" % fields[7]


def main():
    """ Application main. """

    csv_files = glob.glob('*.csv')

    print "======================================================"
    print "BEGIN PROGRAM\n"

    for file_name in csv_files:

        with open(file_name, 'r') as out_file:
            record_list = list(out_file)
        record_list = [s.strip() for s in record_list]

        print "Summary for '" + file_name + "':"

        # sort records by MARE and print optimal record
        record_list.sort(key=lambda s: float(s.split(',')[6]))
        print "\nOptimal MARE result:"
        print_record(record_list[0])

        # sort records by RMSPE and print optimal record
        record_list.sort(key=lambda s: float(s.split(',')[7]))
        print "\nOptimal RMSPE result:"
        print_record(record_list[0])

        if file_name != csv_files[-1]:
            print "\n------------------------------------------------------\n"

    print "\nEND PROGRAM"
    print "======================================================"


if __name__ == "__main__":
    main()
