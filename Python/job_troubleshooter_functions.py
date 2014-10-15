#! /user/bin/env python

import os
import re
import sys
import math
import argparse
import datetime
from time import *
from types import *
from PBSQuery import *
from subprocess import *
from multiprocessing import *

class GlobalVars():
    def __init__(self):
        self.pbs_query = PBSQuery()
        self.job_type = ""
        self.idle_jobs = []
        self.total_jobs = 0.0
        self.job_list_length = 0.0
        self.current_time = mktime(strptime(ctime(time())))  # in seconds
        self.current_year = ctime(time()).split(" ")[4]
        self.system = self.pbs_query.get_server_name().partition("-")[0]
        self.job_server = self.pbs_query.get_server_name()

        self.word = sys.maxsize  # double check word/gb conversions ...
        self.gb_from_mw = 1/(64.0**3)
        self.gb_from_kw = 1/(64.0**2)
        self.gb_from_w = 1/64.0
        self.gb_from_mb = 1/1024.0
        self.gb_from_kb = self.gb_from_mb/1024.0
        self.gb_from_b = self.gb_from_kb/1024.0
        
        self.available_licenses = self.find_available_licenses() 
        self.showres = check_output(["showres"]).strip("\n").split("\n") 

    def find_available_licenses(self):
        # Find all available licenses on the system

        query_flexlm = check_output(["/usr/local/sbin/query-flexlm"]).rstrip("\n").partition("ARES=")[2].split(",")
        self.available_licenses = {}

        for lic in query_flexlm:
            self.available_licenses[lic.partition(":")[0]] = int(lic.partition(":")[2])

global global_vars
global_vars = GlobalVars()

class ProgressBar():
    def __init__(self):
        pass

    def update(self, q): 
        processed_jobs = 0
        percent_complete = int(100*processed_jobs/global_vars.total_jobs)

        arrow = "=>"
        space = "                                        "
        message = "Processing [" + arrow + space + "]" + str(percent_complete) + "%"
                                    
        sys.stdout.write("\r\x1b[K" + message)
        sys.stdout.flush()

        while processed_jobs < min(int(global_vars.total_jobs), int(global_vars.job_list_length)):
            if int(100*processed_jobs/global_vars.total_jobs) >= percent_complete + 5:
                arrow = arrow.rjust(len(arrow) + 1, "=")
                arrow = arrow.rjust(len(arrow) + 1, "=")
                space = space[0:len(space)-2]

            percent_complete = int(100*processed_jobs/global_vars.total_jobs)
            message = "Processing [" + arrow + space + "]" + str(percent_complete) + "%"
            sys.stdout.write("\r\x1b[K" + message)
            sys.stdout.flush()  
            processed_jobs = q.get(True)

        message = "Processing [=======================================>]100%\n\n"
        sys.stdout.write("\r\x1b[K" + message)
        sys.stdout.flush()

class ProcessSingleJob(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        global_vars.total_jobs = 1
        global_vars.job_type = "Single"
        global_vars.idle_jobs.append(Job(values[0]))

class ProcessQueue(argparse.Action):  
    def __call__(self, parser, namespace, values, option_string=None):
        queued_users = []
        processed_jobs = 0

        if type(values) == NoneType:
            global_vars.total_jobs = float(namespace.number_of_jobs)

        else:
            global_vars.total_jobs = float(values)

        # Get a list of idle or blocked jobs

        cmd = "showq"

        if "-b" in option_string or "--blocked" in option_string:
            option = "-b"
            user_index = 1
            global_vars.job_type = "Blocked"

        elif "-i" in option_string or "--idle" in option_string:
            option = "-i"
            user_index = 4
            global_vars.job_type = "Idle"

        showq = check_output([cmd, option])

        try:
            global_vars.job_list_length = int(showq.partition("Total jobs:  ")[2])

        except ValueError:
            print showq
            exit

        else:
            raw_showq = showq.strip("\n").split("\n") 

        if global_vars.job_list_length < 1:
            print "The output of 'showq' with the current option is empty.  Please try again."
            exit

        else:
            # initialize and start status bar in a separate process
            
            q = Queue(1)

            progress_bar = ProgressBar()
            child_process = Process(target=ProgressBar.update, args=(progress_bar,q))
            child_process.start()


            while not re.match('^[0-9]*.*[0-9]$', raw_showq[0]): 
                raw_showq.pop(0)

            for job in raw_showq[0:min(int(global_vars.total_jobs), int(global_vars.job_list_length))]:
                jobid = re.split("\s*", job)[0]
 
                if bool(re.match('^[0-9]*[\*]$', jobid)): 
                    jobid = jobid.split("*")[0]

                user = re.split("\s*", job)[user_index]

                if not user in queued_users:
                    queued_users.append(user)

                global_vars.idle_jobs.append(Job(jobid))
                
                
                processed_jobs += 1
                q.put(processed_jobs, True)

            child_process.join()


class StateResponse():
    def __init__(self, job):
        self.state_response = { 'C' : self.completed,
                                'R' : self.running,
                                'E' : self.exiting,
                                'U' : self.unknown,
                                'Q' : self.queued,
                                'H' : self.held,
                                'T' : self.other,
                                'S' : self.other,
                                'W' : self.other,
                                'System' : self.system_hold,
                                'Batch' : self.batch_hold,
                                'User' : self.user_hold }
        
    def completed(self, job):
        message = job.jobid + " has completed."
        job.report['Scheduler Information:'].append(' - ' + job.jobid + ' has completed.')
    
    def running(self, job):
        message = job.jobid + " is running."
        job.report['Scheduler Information:'].append(' - ' + job.jobid + ' is running.')
    
    def exiting(self, job):
        # if exiting time is too long, there is a problem
        # else job is exiting

        message = job.jobid + " is exiting."
        job.report['Scheduler Information:'].append(' - ' + job.jobid + ' is exiting.')

    def unknown(self, job):
        if job.moab_state['State'] == 'Unknown':
            job.report['Scheduler Information:'].append(' - ' + job.jobid + ' doesn\'t exist.')
 
        elif job.moab_state['State'] == 'Complete':
            job.report['Scheduler Information:'].append(' - ' + job.jobid + ' has completed.')
 
        else:
            job.report['Scheduler Information:'].append(' - An unknown problem has occurred ... Generating more information ...')
            job.get_additional_information()

    def queued(self, job):
        if job.time_diff > 0 and job.time_diff < 300 and job.moab_state['State'] == 'Unknown':
            job.report['Scheduler Information:'].append(' - ' + job.jobid + ' is newly submitted.')
        
        else:
            job.get_additional_information()   

    def held(self, job):
        if job.time_diff > 0 and job.time_diff < 300 and job.moab_state['State'] == 'Unknown':
            job.report['Scheduler Information:'].append(' - ' + job.jobid + ' is newly submitted.')

        elif 'depend' in job.torque_state.keys():
            dependency = (job.torque_state['depend'][0].partition(":")[0], job.torque_state['depend'][0].partition(":")[2].partition(".")[0])
            
            if dependency[0] == "afterok":
                job.report['Scheduler Information:'].append(' - ' + job.jobid + ' depends on the successful completion of job ' + dependency[1])
            elif dependency[0] == "afterany":
                job.report['Scheduler Information:'].append(' - ' + job.jobid + ' can\'t start until job ' + dependency[1] + ' has terminated.')
            elif dependency[0] == "after":
                job.report['Scheduler Information:'].append(' - ' + job.jobid + ' can\'t start until job ' + dependency[1] + 'has started.')
            elif dependency[0] == "afternotok":
                job.report['Scheduler Information:'].append(' - ' + job.jobid + ' won\'t start unless job ' + dependency[1] + ' fails.')
            elif dependency[0] == "on":
                job.report['Scheduler Information:'].append(' - ' + job.jobid + ' won\'t start until ' + dependency[1] + ' jobs have completed.')
            


    def other(self, job):
        job.report['Scheduler Information:'].append('WARNING: An unknown problem has occurred ... Generating more information ...')
        job.get_additional_information()

    def user_hold(self, job):
        job.report['Holds:'].append(' - ' + job.jobid + ' has a User Hold in place.')
    
    def batch_hold(self, job):
        print "Made it"
        job.report['Holds:'].append(' - ' + job.jobid + ' has a Batch Hold in place.')
    
    def system_hold(self, job):
        job.report['Holds:'].append(' - ' + + job.jobid + ' has a System Hold in place.')


class Job():
    def __init__(self, jobid):
        self.jobid = str(jobid)
        self.time_diff = 0
        self.torque_state = global_vars.pbs_query.getjob(str(jobid) + "." + global_vars.job_server)
        #self.get_torque_job_state()
        self.moab_state = {}
        self.get_moab_job_state()
        self.report = {"Scheduler Information:" : [],
                       "Holds:" : [],
                       "Resource Requests:" : [],
                       "License Availability:" : [],
                       "System Information:" : [],
                       "Notes:" : []}
        self.analysis()

    def get_torque_job_state(self):
        fnull = open(os.devnull, "wb")

        try:
            raw_torque_data = re.split("[\n]*", check_output(["qstat", "-f", self.jobid], stderr=fnull).partition("Variable_List")[0])

        except CalledProcessError:
            self.torque_state["job_state"] = 'U'

        else: 
            raw_torque_data.pop(0)

            for item in raw_torque_data:
                (key, val) = (item.partition(" = ")[0].lstrip(" \t\n"), item.partition(" = ")[2])

                if key == "qtime":
                    qtime_sec = mktime(strptime(val, "%a %b %d %H:%M:%S %Y"))
                    self.torque_state[key] = qtime_sec

                else: 
                    if len(key) != 0:
                        self.torque_state[key] = val

        fnull.close()

    def get_moab_job_state(self):
        fnull = open(os.devnull, "wb")

        try:
            raw_moab_data = re.split("\n*", check_output(["checkjob", self.jobid], stderr=fnull))
                                       
        except CalledProcessError:
            self.moab_state["State"] = 'Unknown'

        else:        
            for item in raw_moab_data:
                if re.match("^[\S]*[a-zA-Z0-9\s]*:", item):  # this regex isn't quite right... 
                    (key, val) = (item.partition(":")[0].strip('\s'), item.partition(":")[2].strip('\s'))
                    if key in self.moab_state.keys():
                        self.moab_state[key].append(val)
                    else:
                        self.moab_state[key] = val

        '''
        print raw_moab_data
        print "\n\n"
        print self.moab_state.keys()
        print "\n\n"
        '''
        fnull.close()
        
    def analysis(self):
        try:
            self.time_diff = global_vars.current_time - int(self.torque_state["qtime"][0])

        except KeyError:
            self.time_diff = -1
 
        state = StateResponse(self)
        state.state_response[self.torque_state["job_state"][0]](self)

    def get_additional_information(self):
        # analyze resource requests and other information pertaining to a specific job, as well as system and scheduler information

        if 'NOTE' in self.moab_state.keys():
            if type(self.moab_state['NOTE']) == ListType:
                for note in self.moab_state['NOTE']:
                    self.report['Notes:'].append(' - ' + note + '\n')

            else:
                self.report['Notes:'].append(' - ' + self.moab_state['NOTE'] + '\n')

        if 'BLOCK MSG' in self.moab_state.keys():
            self.report['Scheduler Information:'].append(' - ' + self.moab_state['BLOCK MSG'])

        # Job Resource Requests and Info

        nodes = int(self.torque_state["Resource_List"]["nodes"][0].partition(":")[0])
        ppn = int(self.torque_state["Resource_List"]["nodes"][0].partition("=")[2])
        req_duration = self.torque_state["Resource_List"]["walltime"][0].split(":")

        walltime = 0

        if len(req_duration) <= 3:
            exp = len(req_duration)

        else:
            days = req_duration.pop(0)
            req_duration[0] = req_duration[0] + (days*24)
            exp = 3

        for x in req_duration:
            walltime = walltime + int(x)/float(math.pow(60, exp))
            exp -= 1

        # Analyze disk request, if applicable
        try:
            disk = self.torque_state["Resource_List"]["disk"][0]

        except KeyError:
            disk_num = 0
            disk_magnitude = ""
            
        else:
            if len(disk) > 0:
                disk_num = int(re.split('[a-zA-Z]*', disk)[0])
                disk_magnitude = re.split('^[0-9]*', disk)[1]

                if bool(re.match('[mM][bB]?', disk_magnitude)):
                    disk_num = disk_num * global_vars.gb_from_mb

                elif bool(re.match('[kK][bB]?', disk_magnitude)):
                    disk_num = disk_num * global_vars.gb_from_kb

                elif bool(re.match('[bB]?', disk_magnitude)):
                    disk_num = disk_num * global_vars.gb_from_b

                elif bool(re.match('[mM][wW]?', disk_magnitude)):
                    disk_num = disk_num * global_vars.gb_from_mw

                elif bool(re.match('[kK][wW]?', disk_magnitude)):
                    disk_num = disk_num * global_vars.gb_from_kw

                elif bool(re.match('[wW]?', disk_magnitude)):
                    disk_num = disk_num * global_vars.gb_from_w
            else:
                disk_num = 0
                disk_magnitude = ""

        # Analyze mem request, if applicable
        try:
            mem = self.torque_state["Resource_List"]["mem"][0]

        except KeyError:
            mem_num = 0
            mem_magnitude = ""
            
        else:
            if len(mem) > 0:
                mem_num = int(re.split('[a-zA-Z]*', mem)[0])
                mem_magnitude = re.split('^[0-9]*', mem)[1]

                if bool(re.match('[mM][bB]?', mem_magnitude)):
                    mem_num = mem_num * global_vars.gb_from_mb

                elif bool(re.match('[kK][bB]?', mem_magnitude)):
                    mem_num = mem_num * global_vars.gb_from_kb

                elif bool(re.match('[bB]?', mem_magnitude)):
                    mem_num = mem_num * global_vars.gb_from_b

                elif bool(re.match('[mM][wW]?', mem_magnitude)):
                    mem_num = mem_num * global_vars.gb_from_mw

                elif bool(re.match('[kK][wW]?', mem_magnitude)):
                    mem_num = mem_num * global_vars.gb_from_kw

                elif bool(re.match('[wW]?', mem_magnitude)):
                    mem_num = mem_num * global_vars.gb_from_w

            else:
                mem_num = 0
                mem_magnitude = ""

        # Software request
        try:
            software = self.torque_state["Resource_List"]["software"][0]

        except KeyError:
            software = ""
         
        # "gres" request
        try:
            gres = self.torque_state["x"][0]

        except KeyError:
            gres = ""

        # Determine if the job requests a GPU node

        try:
            gpu = bool(re.match(".*gpu.*", self.torque_state["Resource_List"]["nodes"][0]))

        except AttributeError:
            gpu = False

        # Analyze resource requests by system

        if global_vars.system == "opt":  
            if ppn <= 8:
                if gpu and ppn < 8:
                    problem = " - This job is requesting a partial gpu node on Glenn and will never run.\n"
                    solution = " - Solution: Request ppn = 8.\n"
                    message = problem + solution
                    self.report["Resource Requests:"].append(message)
                if mem_num >= 24:
                    problem = " - This job is requesting a standard node on Glenn, but is also requesting more memory than is available on a standard node.\n"
                    solution = " - Solution: If ppn = 8, remove the memory request.  Otherwise, request an amount of memory proportional to ppn.\n"
                    message = problem + solution
                    self.report["Resource Requests:"].append(message)

                if disk_num >= 392:
                    problem = " - This job is requesting a standard node on Glenn, but is also requesting more disk space than is available on a standard node.\n"
                    solution = " - Solution: ??" # not sure what to recommend here
                    message = problem + solution
                    self.report["Resource Requests:"].append(message)

            elif ppn > 8 and ppn < 16:
                problem = " - This job is requesting between 8 and 16 processors on Glenn, and will never run.\n"
                solution = " - Solution: Request ppn <= 8 for a standard node, or ppn = 16 for a large memory node.\n" 
                message = problem + solution
                self.report["Resource Requests:"].append(message)

            elif ppn == 16:
                if mem_num >= 64:
                    problem = " - This job is requesting a large memory node on Glenn, but is also requesting more memory than is available on a large memory node.\n"
                    solution = " - Solution: If ppn = 16, remove the memory request.  Otherwise, request an amount of memory proportional to ppn.\n"
                    message = problem + solution
                    self.report["Resource Requests:"].append(message)
                if disk_num >= 1862:
                    problem = " - This job is requesting a large memory node on Glenn, but is also requesting more disk space than is available on a large memory node.\n"
                    solution = " - Solution: ??\n" # not sure what to recommend here
                    message = problem + solution
                    self.report["Resource Requests:"].append(message)
            else:
                problem = " - This job is requesting more ppn than is available on any node on Glenn.\n"
                solution = " - Solution: Choose a number of ppn <= 8 or ppn = 16.\n"
                message = problem + solution
                self.report["Resource Requests:"].append(message)

        elif global_vars.system == "oak":
            if disk_num >= 812:
                problem = " - This job is requesting more disk space than is available on any Oakley node.\n"
                solution = " - Solution: ??\n" # not sure what to recommend here
                message = problem + solution
                self.report["Resource Requests:"].append(message)

            if ppn <= 12:
                if mem_num >= 48:
                    problem = " - This job is requesting a standard node on Oakley, but is requesting more memory than is available on a standard node.\n" 
                    solution = " - Solution: Remove the memory request from your script.\n"
                    message = problem + solution
                    self.report["Resource Requests:"].append(message)
            else:
                if mem_num >= 192:
                    problem = " - This job is requesting a large memory node on Oakley, but is requesting more memory than is available on a large memory node.\n"
                    solution = " - Solution:  If ppn = 12, remove the memory request.  Otherwise, request an amount of memory proportional to ppn.\n"
                    message = problem + solution
                    self.report["Resource Requests:"].append(message)
                
        # Check software requests and license availability

        if len(software) > 0:
            software_name = software.partition("+")[0]
            try:
                license_req = int(software.partition("+")[2])
            except IndexError:
                license_req = 1

            if license_req > global_vars.available_licenses[software_name]:
                problem = " - The job is requesting more " + software_name + " licenses than are currently available for use.\n"
                solution = " - Solution: Wait for licenses to become available.\n"
                message = problem + solution
                self.report["License Availability:"].append(message)
               
        # Check reservations
        
        for res in global_vars.showres:
            if "system_downtime" in res:
                downtime = re.split("\s*", res, 7)
                downtime_start = re.split("\s*", downtime[7], 4)
                dt_start = ""

                for x in downtime_start:
                    dt_start = dt_start + x + " "
 
                # for testing purposes, I'm just adding the current year -- I need to make this more general later

                dt_start += global_vars.current_year

                self.report["System Information:"].append(" - A system downtime is scheduled for " + dt_start + ", and may impact queue time for this job.\n")
                
                dt_start = mktime(strptime(dt_start, "%a %b %d %H:%M:%S %Y"))

                if global_vars.current_time + walltime >= dt_start:
                    problem = " - The job's walltime will run into a system downtime reservation.\n"
                    solution = " - Solution: Wait until downtime is over, or resubmit with a shorter walltime.\n"
                    message = problem + solution
                    self.report["System Information:"].append(message)

            
            elif self.jobid in res:
                job_res_time = re.split("\s*", res, 7)
                job_res_time_start = re.split("\s*", job_res_time[7], 4) 
                jrt_start = ""

                for x in job_res_time_start:
                    jrt_start = jrt_start + x + " "

                note = " - Job " + self.jobid + " currently has a reservation in place.\n"
                res_time = "Reservation Time: " + jrt_start 
                message = note + res_time + "\n"
                self.report["System Information:"].append(message)
        

class Project():
    def __init__(self, user):
        self.user = user
        self.RU_bal = 0
        self.member_total_jobs = 0
        self.member_total_procs = 0
        self.proj_total_jobs = 0
        self.proj_total_procs = 0
        self.set_project_information()

    def set_project_information(self):
        # Project Information (RU balance, recent usage, project members and currently running jobs)
        src = subprocess.check_output(["curl", "-s", "-u", global_vars.username + ":" + global_vars.password, "-d", "EB-84y8fTxEONuB%2Bkygq8LyJg%3D%3D=" + 
                                       self.user, "https://staff.osc.edu/cgi-bin/osc/frame/login_id"])
        
        self.RU_bal = src.partition("RU Balance: ")[2].partition("</A>")[0]
        
        proj = src.partition("/cgi-bin/osc/frame/project?")
        proj_url = "https://staff.osc.edu" + proj[1] + proj[2].partition("\" TARGET")[0]
        all_proj_info = subprocess.check_output(["curl", "-s", "-u", global_vars.username + ":" + global_vars.password, proj_url])

        proj_members = all_proj_info.partition("<SELECT NAME=")[2].partition(">")[2].partition("</SELECT>")[0].strip("\n\t ").split("\n")

        proj_job_counts = {"Project Job Count: " : 0,
                           "Project Processor Count: " : 0}

        self.proj_total_jobs = 0
        self.proj_total_procs = 0

        for member in proj_members:
            user_job_counts = {"User Job Count: " : 0,
                               "User Processor Count: " : 0}                         
                          
            self.member_total_jobs = 0
            self.member_total_procs = 0

            username = member.partition("<OPTION VALUE=\"")[2].partition("\"")[0]
            qstat = subprocess.check_output(["qstat", "-u", self.user, "-r"]).split("\n")

            for line in qstat:
                if re.match('^[0-9]', line):
                    self.proj_total_jobs += 1
                    self.member_total_jobs += 1
                    
                    self.proj_total_procs += int(re.split("\s*", line)[6])
                    self.member_total_procs += int(re.split("\s*", line)[6])

        


            
    

        

        
            
