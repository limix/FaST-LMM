import logging
import datetime
import fastlmm.util.runner.azurehelper as commonhelpers #!!!cmk is this the best way to include the code from the Azure python sample's common.helper.py?
import os
import time
import pysnptools.util as pstutil
from fastlmm.util.runner import *
from collections import Counter

try:
    import dill as pickle
except:
    logging.warning("Can't import dill, so won't be able to clusterize lambda expressions. If you try, you'll get this error 'Can't pickle <type 'function'>: attribute lookup __builtin__.function failed'")
    import cPickle as pickle

try:
    import azure.batch.batch_service_client as batch 
    import azure.batch.batch_auth as batchauth 
    import azure.batch.models as batchmodels
    import azure.storage.blob as azureblob
    from fastlmm.util.runner.blobxfer import run_command_string as blobxfer #https://pypi.io/project/blobxfer/

except Exception as exp:
    logging.warning(exp)
    pass

class AzureBatch: # implements IRunner
    def __init__(self, task_count, min_node_count, max_node_count, mkl_num_threads = None, logging_handler=logging.StreamHandler(sys.stdout)):
        logger = logging.getLogger() #!!!cmk similar code elsewhere
        if not logger.handlers:
            logger.setLevel(logging.INFO)
        for h in list(logger.handlers):
            logger.removeHandler(h)
        if logger.level == logging.NOTSET or logger.level > logging.INFO:
            logger.setLevel(logging.INFO)
        logger.addHandler(logging_handler)

        self.taskcount = task_count
        self.min_node_count = min_node_count
        self.max_node_count = max_node_count
        self.mkl_num_threads = mkl_num_threads

    def run(self, distributable):
        JustCheckExists().input(distributable) #!!!cmk move input files
        batch_service_url, batch_account, batch_key, storage_account, storage_key = [s.strip() for s in open(os.path.expanduser("~")+"/azurebatch/cred.txt").xreadlines()] #!!!cmk make this a param????

        ####################################################
        # Pickle the thing-to-run
        ####################################################
        run_dir_rel = os.path.join("runs",util.datestamp(appendrandom=True))
        util.create_directory_if_necessary(run_dir_rel, isfile=False)
        distributablep_filename = os.path.join(run_dir_rel, "distributable.p")
        with open(distributablep_filename, mode='wb') as f:
            pickle.dump(distributable, f, pickle.HIGHEST_PROTOCOL)

        ####################################################
        # Copy (update) any input files to the blob and create scripts for running on the nodes
        ####################################################
        inputOutputCopier = AzureBatchCopier("inputfiles",storage_key, storage_account)
        inputOutputCopier.input(distributable)

        script_list = ["",""]
        inputOutputCopier2 = AzureBatchCopierNodeLocal("inputfiles",storage_key, storage_account,script_list)
        inputOutputCopier2.input(distributable)
        inputOutputCopier2.output(distributable)

        ####################################################
        # Create the batch program to run
        ####################################################
        for i, bat_filename in enumerate(["map.bat","reduce.bat"]):
            dist_filename = os.path.join(run_dir_rel, bat_filename)
            with open(dist_filename, mode='w') as f:
                f.write(r"""set path=%AZ_BATCH_APP_PACKAGE_ANACONDA2%\Anaconda2;%AZ_BATCH_APP_PACKAGE_ANACONDA2%\Anaconda2\scripts\;%path%
{6}mkdir ..\output\
{6}cd ..\output\
{6}python.exe ..\wd\blobxfer.py --delete --storageaccountkey {2} --download {3} output . --remoteresource .
{6}cd ..\wd\
{7}
python.exe blobxfer.py --skipskip --delete --storageaccountkey {2} --download {3} pp0 c:\user\tasks\workitems\pps\pp0 --remoteresource .
set pythonpath=c:\user\tasks\workitems\pps\pp0
python.exe %AZ_BATCH_APP_PACKAGE_ANACONDA2%\Anaconda2\Lib\site-packages\fastlmm\util\distributable.py distributable.p LocalInParts(%1,{0},result_file=\"../output/result.p\",mkl_num_threads={1},temp_dir={4})
{6}{8}
cd ..\output\
{5}python.exe ..\wd\blobxfer.py --storageaccountkey {2} --upload {3} output .
{6}python.exe ..\wd\blobxfer.py --storageaccountkey {2} --upload {3} output result.p
                """
                .format(
                    self.taskcount,                         #0
                    self.mkl_num_threads,                   #1
                    storage_key,                            #2 #!!!cmk use the URL instead of the key
                    storage_account,                        #3
                    r'\"../output\"',                       #4
                    "" if i==0 else "@rem ",                #5
                    "" if i==1 else "@rem ",                #6
                    script_list[0],                         #7
                    script_list[1],                         #8
                ))#!!!cmk need multiple blobxfer lines

        ####################################################
        # Upload the thing-to-run to a blob and the blobxfer program
        ####################################################
        block_blob_client = azureblob.BlockBlobService(account_name=storage_account,account_key=storage_key)
        block_blob_client.create_container('application', fail_on_exist=False) #!!!cmk subfolders for each run
        distributablep_url = commonhelpers.upload_blob_and_create_sas(block_blob_client, 'application', "distributable.p", distributablep_filename, datetime.datetime.utcnow() + datetime.timedelta(hours=1))
        blobxfer_fn = os.path.join(os.path.dirname(__file__),"blobxfer.py")
        blobxfer_url = commonhelpers.upload_blob_and_create_sas(block_blob_client, 'application', "blobxfer.py", blobxfer_fn, datetime.datetime.utcnow() + datetime.timedelta(hours=1))
        map_url = commonhelpers.upload_blob_and_create_sas(block_blob_client, 'application', "map.bat", os.path.join(run_dir_rel, "map.bat"), datetime.datetime.utcnow() + datetime.timedelta(hours=1))
        reduce_url = commonhelpers.upload_blob_and_create_sas(block_blob_client, 'application', "reduce.bat", os.path.join(run_dir_rel, "reduce.bat"), datetime.datetime.utcnow() + datetime.timedelta(hours=1))


        ####################################################
        # Copy everything on PYTHONPATH to a blob
        ####################################################
        localpythonpath = os.environ.get("PYTHONPATH") #!!should it be able to work without pythonpath being set (e.g. if there was just one file)? Also, is None really the return or is it an exception.
        if localpythonpath == None: raise Exception("Expect local machine to have 'pythonpath' set")
        for i, localpathpart in enumerate(localpythonpath.split(';')):
            blobxfer(r"blobxfer.py --skipskip --delete --storageaccountkey {} --upload {} {} {}".format(storage_key,storage_account,"pp{}".format(i),"."),
                     wd=localpathpart)
    

        ####################################################
        # Set the pool's autoscale
        # http://azure-sdk-for-python.readthedocs.io/en/dev/batch.html
        # https://azure.microsoft.com/en-us/documentation/articles/batch-automatic-scaling/ (enable after)
        # https://azure.microsoft.com/en-us/documentation/articles/batch-parallel-node-tasks/
        ####################################################
        credentials = batchauth.SharedKeyCredentials(batch_account, batch_key)
        batch_client = batch.BatchServiceClient(credentials,base_url=batch_service_url)
        auto_scale_formula=r"""// Get pending tasks for the past 15 minutes.
$Samples = $ActiveTasks.GetSamplePercent(TimeInterval_Minute * 15);
// If we have fewer than 70 percent data points, we use the last sample point, otherwise we use the maximum of
// last sample point and the history average.
$Tasks = $Samples < 70 ? max(0,$ActiveTasks.GetSample(1)) : max( $ActiveTasks.GetSample(1), avg($ActiveTasks.GetSample(TimeInterval_Minute * 15)));
// If number of pending tasks is not 0, set targetVM to pending tasks, otherwise half of current dedicated.
$TargetVMs = $Tasks > 0? $Tasks:max(0, $TargetDedicated/2);
// The pool size is capped at 20, if target VM value is more than that, set it to 20. This value
// should be adjusted according to your use case.
$TargetDedicated = max({0},min($TargetVMs,{1}));
// Set node deallocation mode - keep nodes active only until tasks finish
$NodeDeallocationOption = taskcompletion;
""".format(self.min_node_count,self.max_node_count)
        #batch_client.pool.enable_auto_scale(
        #        "twoa1",
        #        auto_scale_formula=auto_scale_formula,
        #        auto_scale_evaluation_interval=datetime.timedelta(minutes=10) 
        #    )


        ####################################################
        # Create a job with tasks and run it.
        ####################################################
        job_id = commonhelpers.generate_unique_resource_name(distributable.name)
        job = batchmodels.JobAddParameter(id=job_id, pool_info=batch.models.PoolInformation(pool_id="twoa1"),uses_task_dependencies=True)
        batch_client.job.add(job)

        resource_files=[batchmodels.ResourceFile(blob_source=distributablep_url, file_path="distributable.p"),
                batchmodels.ResourceFile(blob_source=blobxfer_url, file_path="blobxfer.py"),
                batchmodels.ResourceFile(blob_source=map_url, file_path="map.bat"),
                batchmodels.ResourceFile(blob_source=reduce_url, file_path="reduce.bat"),
                ]
        task_list = []
        for taskindex in xrange(self.taskcount):
            map_task = batchmodels.TaskAddParameter(
                id=str(taskindex),
                run_elevated=True,
                resource_files=resource_files,
                command_line="map.bat {0}".format(taskindex),
            )
            task_list.append(map_task)
        reduce_task = batchmodels.TaskAddParameter(
            id="reduce",
            run_elevated=True,
            resource_files=resource_files,
            command_line="reduce.bat {0}".format(self.taskcount),
            depends_on = batchmodels.TaskDependencies(task_id_ranges=[batchmodels.TaskIdRange(0,self.taskcount-1)])
            )
        task_list.append(reduce_task)

        try:
            batch_client.task.add_collection(job_id, task_list)
        except Exception as exception:
            print exception
            raise exception
 
        sleep_sec = 5
        while True:
            logging.info("again")
            tasks = batch_client.task.list(job_id)
            counter = Counter(task.state.value for task in tasks)
            for state, count in counter.iteritems():
                logging.info("{0}: {1}".format(state, count))
            if counter['completed'] == sum(counter.values()) :
                break
            time.sleep(sleep_sec)
            sleep_sec = min(sleep_sec * 1.1,60)
 
 
        #tasks = batch_client.task.list(job_id) 
        #task_ids = [map_task.id for map_task in tasks]
        #commonhelpers.print_task_output(batch_client, job_id, task_ids)

        ####################################################
        # Copy (update) any output files from the blob
        ####################################################
        inputOutputCopier.output(distributable)

        ####################################################
        # Download and Unpickle the result
        ####################################################
        resultp_filename = os.path.join(run_dir_rel, "result.p")
        blobxfer(r"blobxfer.py --storageaccountkey {} --download {} output . --remoteresource result.p".format(storage_key,storage_account), wd=run_dir_rel)
        with open(resultp_filename, mode='rb') as f:
            result = pickle.load(f)
        return result

class AzureBatchCopier(object): #Implements ICopier

    def __init__(self, container, storage_key, storage_account):
        self.container = container
        self.storage_key=storage_key
        self.storage_account=storage_account

    def input(self,item):
        if isinstance(item, str):
            itemnorm = "./"+os.path.normpath(item)
            blobxfer(r"blobxfer.py --skipskip --storageaccountkey {} --upload {} {} {}".format(self.storage_key,self.storage_account,self.container,itemnorm),wd=".")
        elif hasattr(item,"copyinputs"):
            item.copyinputs(self)
        # else -- do nothing

    def output(self,item):
        if isinstance(item, str):
            itemnorm = "./"+os.path.normpath(item)
            blobxfer(r"blobxfer.py --skipskip --storageaccountkey {} --download {} {} {} --remoteresource {}".format(self.storage_key,self.storage_account,self.container, ".", itemnorm), wd=".")
            print "test!!!cmk"
        elif hasattr(item,"copyoutputs"):
            item.copyoutputs(self)
        # else -- do nothing

class AzureBatchCopierNodeLocal(object): #Implements ICopier

    def __init__(self, container, storage_key, storage_account, script_list):
        assert len(script_list) == 2, "expect script list to be a list of length two"
        script_list[0] = ""
        script_list[1] = ""
        self.container = container
        self.storage_key=storage_key
        self.storage_account=storage_account
        self.script_list = script_list

    def input(self,item):
        if isinstance(item, str):
            itemnorm = "./"+os.path.normpath(item)
            self.script_list[0] += r"python.exe ..\wd\blobxfer.py --storageaccountkey {} --download {} {} {} --remoteresource {}{}".format(self.storage_key,self.storage_account,self.container, ".", itemnorm, os.linesep)
        elif hasattr(item,"copyinputs"):
            item.copyinputs(self)
        # else -- do nothing

    def output(self,item):
        if isinstance(item, str):
            itemnorm = "./"+os.path.normpath(item)
            self.script_list[1] += r"python.exe ..\wd\blobxfer.py --storageaccountkey {} --upload {} {} {} --remoteresource {}{}".format(self.storage_key,self.storage_account,self.container, ".", itemnorm, os.linesep)
        elif hasattr(item,"copyoutputs"):
            item.copyoutputs(self)
        # else -- do nothing


def test_fun(runner):
    from fastlmm.util.mapreduce import map_reduce
    import shutil

    os.chdir(r"c:\deldir\del1")

    def printx(x):
        a =  os.path.getsize("data/del2.txt")
        b =  os.path.getsize("data/del3/text.txt")
        print x**2
        return [x**2, a, b, "hello"]

    def reducerx(sequence):
        shutil.copy2('data/del3/text.txt', 'data/del3/text2.txt')
        return list(sequence)

    result = map_reduce(range(1000),
                        mapper=printx,
                        reducer=reducerx,
                        name="printx",
                        input_files=["data/del2.txt","data/del3/text.txt"],
                        output_files=["data/del3/text2.txt"],
                        runner = runner
                        )
    print result
    print "done"

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Hello")

    if False:
        pass
    elif False:
        pstutil.create_directory_if_necessary(r"..\deldir\result.p")
    elif False:
        from fastlmm.util.runner.blobxfer import main as blobxfermain #https://pypi.io/project/blobxfer/
        batch_service_url, batch_account, batch_key, storage_account, storage_key = [s.strip() for s in open(os.path.expanduser("~")+"/azurebatch/cred.txt").xreadlines()] #!!!cmk make this a param????
        os.chdir(r"c:\deldir\sub")
        c = r"IGNORED --delete --storageaccountkey {} --upload {} pp2 .".format(storage_key, storage_account)
        sys.argv = c.split(" ")
        blobxfermain(exit_is_ok=False)
        print "done"

    elif False: # How to copy a directory to a blob -- only copying new stuff and remove any old stuff
        from fastlmm.util.runner.blobxfer import main as blobxfermain #https://pypi.io/project/blobxfer/
        batch_service_url, batch_account, batch_key, storage_account, storage_key = [s.strip() for s in open(os.path.expanduser("~")+"/azurebatch/cred.txt").xreadlines()] #!!!cmk make this a param????

        localpythonpath = os.environ.get("PYTHONPATH") #!!should it be able to work without pythonpath being set (e.g. if there was just one file)? Also, is None really the return or is it an exception.
        if localpythonpath == None: raise Exception("Expect local machine to have 'pythonpath' set")
        for i, localpathpart in enumerate(localpythonpath.split(';')):
            os.chdir(localpathpart) #!!!cmk at the end put back where it was
            c = r"blobxfer.py --storageaccountkey {} --upload {} {} {}".format(storage_key,storage_account,"test{}".format(i),".")
            sys.argv = c.split(" ")
            blobxfermain(exit_is_ok=False)


        print "done"


    elif False:

        #Expect:
        # batch service url, e.g., https://fastlmm.westus.batch.azure.com
        # account, e.g., fastlmm
        # key, e.g. Wsz....

        batch_service_url, batch_account, batch_key, storage_account, storage_key = [s.strip() for s in open(os.path.expanduser("~")+"/azurebatch/cred.txt").xreadlines()]

        #https://azure.microsoft.com/en-us/documentation/articles/batch-python-tutorial/
        # Create the blob client, for use in obtaining references to
        # blob storage containers and uploading files to containers.
        block_blob_client = azureblob.BlockBlobService(account_name=storage_account,account_key=storage_key)

        # Use the blob client to create the containers in Azure Storage if they
        # don't yet exist.
        app_container_name = 'application'
        input_container_name = 'input'
        output_container_name = 'output'
        block_blob_client.create_container(app_container_name, fail_on_exist=False)
        block_blob_client.create_container(input_container_name, fail_on_exist=False)
        block_blob_client.create_container(output_container_name, fail_on_exist=False)

        sas_url = commonhelpers.upload_blob_and_create_sas(block_blob_client, app_container_name, "delme.py", r"C:\Source\carlk\fastlmm\fastlmm\util\runner\delme.py", datetime.datetime.utcnow() + datetime.timedelta(hours=1))  



        job_id = commonhelpers.generate_unique_resource_name("HelloWorld")

    
        credentials = batchauth.SharedKeyCredentials(batch_account, batch_key)


        batch_client = batch.BatchServiceClient(credentials,base_url=batch_service_url)

        job = batchmodels.JobAddParameter(id=job_id, pool_info=batch.models.PoolInformation(pool_id="twoa1"))
        batch_client.job.add(job)

       # see http://azure-sdk-for-python.readthedocs.io/en/latest/ref/azure.batch.html
       # http://azure-sdk-for-python.readthedocs.io/en/latest/ref/azure.batch.models.html?highlight=TaskAddParameter
       #  http://azure-sdk-for-python.readthedocs.io/en/latest/_modules/azure/batch/models/task_add_parameter.html
        task = batchmodels.TaskAddParameter(
            id="HelloWorld",
            run_elevated=True,
            resource_files=[batchmodels.ResourceFile(blob_source=sas_url, file_path="delme.py")],
            command_line=r"c:\Anaconda2\python.exe delme.py",
            #doesn't work command_line=r"python c:\user\tasks\shared\test.py"
            #works command_line=r"cmd /c c:\Anaconda2\python.exe c:\user\tasks\shared\test.py"
            #command_line=r"cmd /c python c:\user\tasks\shared\test.py"
            #command_line=r"cmd /c c:\user\tasks\testbat.bat"
            #command_line=r"cmd /c echo start & c:\Anaconda2\python.exe -c 3/0 & echo Done"
            #command_line=r"c:\Anaconda2\python.exe -c print('ello')"
            #command_line=r"python -c print('hello_from_python')"
            #command_line=commonhelpers.wrap_commands_in_shell('windows', ["python -c print('hello_from_python')"]
        )

        try:
            batch_client.task.add_collection(job_id, [task])
        except Exception as exception:
            print exception
 
        commonhelpers.wait_for_tasks_to_complete(batch_client, job_id, datetime.timedelta(minutes=25))
 
 
        tasks = batch_client.task.list(job_id) 
        task_ids = [task.id for task in tasks]
 
 
        commonhelpers.print_task_output(batch_client, job_id, task_ids)
    elif True:
        from fastlmm.util.runner.AzureBatch import test_fun
        from fastlmm.util.runner import Local, HPC, LocalMultiProc

        runner = AzureBatch(task_count=20,min_node_count=2,max_node_count=7) #!!!cmk there is a default core limit of 99
        #runner = LocalMultiProc(2)
        test_fun(runner)

# Copy input files to the machines
# copy output files from the machine
# make sure every use of storage: locally, in blobs, and on nodes, is sensable
# make sure that things stop when there is an error
# If multiple jobs run on the same machine, only download the data once.
# replace AzureBatchCopier("inputfiles" with something more automatic, based on local files
# When there is an error, say so, don't just return the result from the previous good run
# Look at # https://azure.microsoft.com/en-us/documentation/articles/batch-parallel-node-tasks/
# Can run multiple jobs at once and they don't clobber each other
# Copy 2+ python path to the machines
# Understand HDFS and Azure storage
# Stop using fastlmm2 for storage
# control the default core limit so can have more than taskcount=99
# is 'datetime.timedelta(minutes=25)' right?

# DONE Don't copy stdout stderr back
# DONE instead of Datetime with no tzinfo will be considered UTC.
#            Checking if all tasks are complete...
#      tell how many tasks finished, running, waiting
# DONE Upload of answers is too noisy
# DONE more than 2 machines (grow)
# DONE Faster install of Python
# DONE See http://gonzowins.com/2015/11/06/deploying-apps-into-azurebatch/ from how copy zip and then unzip
# DONE            also https://www.opsgility.com/blog/2012/11/08/bootstrapping-a-virtual-machine-with-windows-azure/        
# DONE copy results back to blog storage
# DONE Create a reduce job that depends on the results
# DONE Copy 1 python path to the machines
# DONE # copy python program to machine and run it
# DONE Install Python manully on both machines and then run a python cmd on the machines
# DONE get iDistribute working on just two machines with pytnon already installed and no input files, just seralized input and seralized output
