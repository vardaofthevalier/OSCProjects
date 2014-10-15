#!/usr/bin/env python

# import declarations

import os
import sys

# Check python version -- must be 2.7 or greater for some modules

if sys.version_info < (2, 7):
    print "This utility requires Python 2.7 or greater."
    exit(-1)

import time
import argparse
from subprocess import *
from job_troubleshooter_functions import *

def main():
    # Parse and process command line arguments
    parser = argparse.ArgumentParser(description='Diagnose problems with currently queued or blocked jobs.')
    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument('-s', '--single', action=ProcessSingleJob, nargs=1, type=int, dest='jobid')
    group.add_argument('-i', '--idle', action=ProcessQueue, nargs='?', default='20', type=int, dest='number_of_jobs')
    group.add_argument('-b', '--blocked', action=ProcessQueue, nargs='?', default='20', type=int, dest='number_of_jobs')
    parser.add_argument('-v', '--verbose', action='store_true', dest='verbose', default=False)

    namespace = parser.parse_args()
    
    # Print contents of job report to the screen and to a file

    today = datetime.datetime.today()
    date = today.strftime("%Y-%m-%d")

    filename = global_vars.job_type + "_Job_Report_" + date + ".txt"
    f = open(filename, "wb")

    print "***** " + global_vars.job_type + " Job Report (" + global_vars.system + ") " + date + " *****\n"
    print "\n"
    f.write("***** " + global_vars.job_type + " Job Report (" + global_vars.system + ") " + date + " *****\n")
    f.write("\n")

    for job in global_vars.idle_jobs:
        print "Job ID: " + job.jobid
        f.write("Job ID: " + job.jobid + "\n")
        print "\n"
        f.write("\n")

        if namespace.verbose == True:
            print "Information from Torque: \n"
            f.write("Information from Torque: \n\n")
        
            for key in job.torque_state.keys():
                print "\t" + str(key) + " : " + str(job.torque_state[key]) 
                f.write("\t" + str(key) + " : " + str(job.torque_state[key]) + "\n")

            print "\n"
            f.write("\n")

            print "Information from Moab: \n"
            f.write("Information from Moab: \n\n")

            for key in job.moab_state.keys():
                print "\t" + str(key) + " : " + str(job.moab_state[key]) 
                f.write("\t" + str(key) + " : " + str(job.moab_state[key]) + "\n")

            print "\n"
            f.write("\n")

        empty_lists = 0
        for key in job.report.keys():
            if len(job.report[key]) > 0:
                f.write(key + "\n")
                print key + "\n"
                for message in job.report[key]:
                    f.write(message + "\n")
                    print message
                f.write("\n")
                print "\n"
            else:
                empty_lists += 1

        if empty_lists == len(job.report.keys()):
            message = " - Nothing unusual was discovered about this job"
            f.write(message)
            print message
        

        print "*****************************************\n"
        f.write("*****************************************\n")
        

    f.close()

    
if __name__ == "__main__":
    main()


    
