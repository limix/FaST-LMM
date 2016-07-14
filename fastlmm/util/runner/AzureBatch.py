import logging
import datetime
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
    import azure.batch.models as batchmodels
    import azure.storage.blob as azureblob
    azure_ok = True
except Exception, exception:
    logging.warning("Can't import azure, so won't be able to clusterize to azure")
    azure_ok = False

if azure_ok:
    import azurehelper as commonhelpers #!!!cmk is this the best way to include the code from the Azure python sample's common.helper.py?
    import azure.batch.batch_service_client as batch 
    import azure.batch.batch_auth as batchauth 
    from fastlmm.util.runner.blobxfer import run_command_string as blobxfer #https://pypi.io/project/blobxfer/

class AzureBatch: # implements IRunner
    def __init__(self, task_count, min_node_count, max_node_count, pool_id, mkl_num_threads = None,
                 logging_handler=logging.StreamHandler(sys.stdout)):
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
        self.pool_id = pool_id

    def run(self, distributable):
        JustCheckExists().input(distributable) #!!!cmk move input files
        batch_service_url, batch_account, batch_key, storage_account, storage_key = [s.strip() for s in open(os.path.expanduser("~")+"/azurebatch/cred.txt").xreadlines()] #!!!cmk make this a param????

        container = "mapreduce" #!!!cmk make this an option
        utils_version = 0        #!!!cmk make this an option
        pp_version = 0
        data_version = 0

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
        data_blob_fn = "{0}-data-v{1}".format(container,data_version)
        inputOutputCopier = AzureBatchCopier(data_blob_fn, storage_key, storage_account)
        inputOutputCopier.input(distributable)

        script_list = ["",""]
        inputOutputCopier2 = AzureBatchCopierNodeLocal(data_blob_fn, container, data_version ,storage_key, storage_account,script_list)
        inputOutputCopier2.input(distributable)
        inputOutputCopier2.output(distributable)

        ####################################################
        # Create the jobprep program
        ####################################################
        localpythonpath = os.environ.get("PYTHONPATH") #!!should it be able to work without pythonpath being set (e.g. if there was just one file)? Also, is None really the return or is it an exception.
        dist_filename = os.path.join(run_dir_rel, "jobprep.bat")
        with open(dist_filename, mode='w') as f2:
            f2.write(r"""set
set path=%AZ_BATCH_APP_PACKAGE_ANACONDA2%\Anaconda2;%AZ_BATCH_APP_PACKAGE_ANACONDA2%\Anaconda2\scripts\;%path%
FOR /L %%i IN (0,1,{7}) DO python.exe %AZ_BATCH_TASK_WORKING_DIR%\blobxfer.py --skipskip --delete --storageaccountkey {2} --download {3} {4}-pp-v{5}-%%i %AZ_BATCH_NODE_SHARED_DIR%\{4}\pp\v{5}\%%i --remoteresource .
{6}
mkdir %AZ_BATCH_TASK_WORKING_DIR%\..\..\output
            """
            .format(
                self.taskcount,                         #0
                self.mkl_num_threads,                   #1
                storage_key,                            #2 #!!!cmk use the URL instead of the key
                storage_account,                        #3
                container,                              #4
                pp_version,                             #5
                script_list[0],                         #6
                len(localpythonpath.split(';'))-1,      #7
            ))#!!!cmk need multiple blobxfer lines


        ####################################################
        # Create the batch program to run
        ####################################################
        output_blobfn = "{}/output".format(run_dir_rel.replace("\\","/"))
        for i, bat_filename in enumerate(["map.bat","reduce.bat"]):
            dist_filename = os.path.join(run_dir_rel, bat_filename)
            with open(dist_filename, mode='w') as f1:
                f1.write(r"""set path=%AZ_BATCH_APP_PACKAGE_ANACONDA2%\Anaconda2;%AZ_BATCH_APP_PACKAGE_ANACONDA2%\Anaconda2\scripts\;%path%
{6}cd %AZ_BATCH_TASK_WORKING_DIR%\..\..\output
{6}FOR /L %%i IN (0,1,{11}) DO python.exe %AZ_BATCH_JOB_PREP_WORKING_DIR%\blobxfer.py --storageaccountkey {2} --download {3} {8}/{10} . --remoteresource %%i.{0}.p
cd %AZ_BATCH_NODE_SHARED_DIR%\{8}\data\v{9}
set pythonpath=%AZ_BATCH_NODE_SHARED_DIR%\mapreduce\pp\v0\0
python.exe %AZ_BATCH_APP_PACKAGE_ANACONDA2%\Anaconda2\Lib\site-packages\fastlmm\util\distributable.py %AZ_BATCH_JOB_PREP_WORKING_DIR%\distributable.p LocalInParts(%1,{0},result_file=r\"{4}/result.p\",mkl_num_threads={1},temp_dir=r\"{4}\")
{6}{7}
cd %AZ_BATCH_TASK_WORKING_DIR%\..\..\output
{5}python.exe %AZ_BATCH_JOB_PREP_WORKING_DIR%\blobxfer.py --storageaccountkey {2} --upload {3} {8} %1.{0}.p --remoteresource {10}/%1.{0}.p
{6}python.exe %AZ_BATCH_JOB_PREP_WORKING_DIR%\blobxfer.py --storageaccountkey {2} --upload {3} {8} result.p --remoteresource {10}/result.p
                """
                .format(
                    self.taskcount,                         #0
                    self.mkl_num_threads,                   #1
                    storage_key,                            #2 #!!!cmk use the URL instead of the key
                    storage_account,                        #3
                    "%AZ_BATCH_TASK_WORKING_DIR%/../../output", #4
                    "" if i==0 else "@rem ",                #5
                    "" if i==1 else "@rem ",                #6
                    script_list[1],                         #7
                    container,                              #8
                    data_version,                           #9
                    output_blobfn,                          #10
                    self.taskcount-1,                       #11
                ))#!!!cmk need multiple blobxfer lines

        ####################################################
        # Upload the thing-to-run to a blob and the blobxfer program
        ####################################################
        block_blob_client = azureblob.BlockBlobService(account_name=storage_account,account_key=storage_key)
        block_blob_client.create_container(container, fail_on_exist=False)

        distributablep_blobfn = "{}/distributable.p".format(run_dir_rel.replace("\\","/"))
        distributablep_url = commonhelpers.upload_blob_and_create_sas(block_blob_client, container, distributablep_blobfn, distributablep_filename, datetime.datetime.utcnow() + datetime.timedelta(hours=1))

        blobxfer_blobfn = "utils/v{}/blobxfer.py".format(utils_version)
        blobxfer_url   = commonhelpers.upload_blob_and_create_sas(block_blob_client, container, blobxfer_blobfn, os.path.join(os.path.dirname(__file__),"blobxfer.py"), datetime.datetime.utcnow() + datetime.timedelta(hours=1))

        jobprep_blobfn = "{}/jobprep.bat".format(run_dir_rel.replace("\\","/"))
        jobprepbat_url = commonhelpers.upload_blob_and_create_sas(block_blob_client, container, jobprep_blobfn, os.path.join(run_dir_rel, "jobprep.bat"), datetime.datetime.utcnow() + datetime.timedelta(hours=1))

        map_blobfn = "{}/map.bat".format(run_dir_rel.replace("\\","/"))
        map_url = commonhelpers.upload_blob_and_create_sas(block_blob_client, container, map_blobfn, os.path.join(run_dir_rel, "map.bat"), datetime.datetime.utcnow() + datetime.timedelta(hours=1))

        reduce_blobfn = "{}/reduce.bat".format(run_dir_rel.replace("\\","/"))
        reduce_url = commonhelpers.upload_blob_and_create_sas(block_blob_client, container, reduce_blobfn, os.path.join(run_dir_rel, "reduce.bat"), datetime.datetime.utcnow() + datetime.timedelta(hours=1))


        ####################################################
        # Copy everything on PYTHONPATH to a blob
        ####################################################
        if localpythonpath == None: raise Exception("Expect local machine to have 'pythonpath' set")
        for i, localpathpart in enumerate(localpythonpath.split(';')):
            blobxfer(r"blobxfer.py --skipskip --delete --storageaccountkey {0} --upload {1} {2}-pp-v{3}-{4} .".format(
                            storage_key,                    #0
                            storage_account,                #1
                            container,                      #2
                            pp_version,                     #3
                            i,                              #4
                            ),
                     wd=localpathpart)
    

        ####################################################
        # Set the pool's autoscale
        # http://azure-sdk-for-python.readthedocs.io/en/dev/batch.html
        # https://azure.microsoft.com/en-us/documentation/articles/batch-automatic-scaling/ (enable after)
        # https://azure.microsoft.com/en-us/documentation/articles/batch-parallel-node-tasks/
        ####################################################
        credentials = batchauth.SharedKeyCredentials(batch_account, batch_key)
        batch_client = batch.BatchServiceClient(credentials,base_url=batch_service_url)

        #!!!cmk document that maxTasksPerNode and packing policy are per-pool and setting the values will over ride previous values

        if False: #!!!cmk turned off while debugging so can remote into VMs without them be taken away
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
            batch_client.pool.enable_auto_scale(
                    self.pool_id,
                    auto_scale_formula=auto_scale_formula,
                    auto_scale_evaluation_interval=datetime.timedelta(minutes=10) 
                )

        ####################################################
        # Create a job with a job prep task
        ####################################################
        job_id = commonhelpers.generate_unique_resource_name(distributable.tempdirectory.replace("_","-")) #!!!cmk is the replace needed? Is it enough?

        job_preparation_task = batchmodels.JobPreparationTask(
                id="jobprep",
                run_elevated=True,
                resource_files=[
                    batchmodels.ResourceFile(blob_source=blobxfer_url, file_path="blobxfer.py"),
                    batchmodels.ResourceFile(blob_source=jobprepbat_url, file_path="jobprep.bat"),
                    batchmodels.ResourceFile(blob_source=distributablep_url, file_path="distributable.p"),
                    ],
                command_line="jobprep.bat",
                )

        job = batchmodels.JobAddParameter(
            id=job_id,
            job_preparation_task=job_preparation_task,
            pool_info=batch.models.PoolInformation(pool_id=self.pool_id),
            uses_task_dependencies=True)
        batch_client.job.add(job)

        ####################################################
        # Add regular tasks to the job and run it.
        ####################################################
        task_list = []
        for taskindex in xrange(self.taskcount):
            map_task = batchmodels.TaskAddParameter(
                id=str(taskindex),
                run_elevated=True,
                resource_files=[batchmodels.ResourceFile(blob_source=map_url, file_path="map.bat")],
                command_line=r"map.bat {0}".format(taskindex),
            )
            task_list.append(map_task)
        reduce_task = batchmodels.TaskAddParameter(
            id="reduce",
            run_elevated=True,
            resource_files=[batchmodels.ResourceFile(blob_source=reduce_url, file_path="reduce.bat")],
            command_line=r"reduce.bat {0}".format(self.taskcount),
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
        blobxfer(r"blobxfer.py --storageaccountkey {0} --download {1} {2}/{3} . --remoteresource result.p".format(storage_key,storage_account,container,output_blobfn), wd=run_dir_rel)
        resultp_filename = os.path.join(run_dir_rel, "result.p")
        with open(resultp_filename, mode='rb') as f:
            result = pickle.load(f)
        return result

class AzureBatchCopier(object): #Implements ICopier

    def __init__(self, blob_fn, storage_key, storage_account):
        self.blob_fn = blob_fn
        self.storage_key=storage_key
        self.storage_account=storage_account

    def input(self,item):
        if isinstance(item, str):
            itemnorm = "./"+os.path.normpath(item).replace("\\","/")
            blobxfer(r"blobxfer.py --skipskip --storageaccountkey {} --upload {} {} {}".format(self.storage_key,self.storage_account,self.blob_fn,itemnorm),wd=".")
        elif hasattr(item,"copyinputs"):
            item.copyinputs(self)
        # else -- do nothing

    def output(self,item):
        if isinstance(item, str):
            itemnorm = "./"+os.path.normpath(item).replace("\\","/")
            blobxfer(r"blobxfer.py --skipskip --storageaccountkey {} --download {} {} {} --remoteresource {}".format(self.storage_key,self.storage_account,self.blob_fn, ".", itemnorm), wd=".")
        elif hasattr(item,"copyoutputs"):
            item.copyoutputs(self)
        # else -- do nothing

class AzureBatchCopierNodeLocal(object): #Implements ICopier

    def __init__(self, datablob_fn, container, data_version, storage_key, storage_account, script_list):
        assert len(script_list) == 2, "expect script list to be a list of length two"
        script_list[0] = ""
        script_list[1] = ""
        self.datablob_fn = datablob_fn
        self.container = container
        self.data_version = data_version
        self.storage_key=storage_key
        self.storage_account=storage_account
        self.script_list = script_list

    def input(self,item):
        if isinstance(item, str):
            itemnorm = "./"+os.path.normpath(item).replace("\\","/")
            node_folder = r"%AZ_BATCH_NODE_SHARED_DIR%\{0}\data\v{1}".format(self.container,self.data_version)
            self.script_list[0] += r"mkdir {0}{1}".format(node_folder,os.linesep)
            self.script_list[0] += r"cd {0}{1}".format(node_folder,os.linesep)
            self.script_list[0] += r"python.exe %AZ_BATCH_TASK_WORKING_DIR%\blobxfer.py --storageaccountkey {} --download {} {} {} --remoteresource {}{}".format(self.storage_key,self.storage_account,self.datablob_fn, ".", itemnorm, os.linesep)
        elif hasattr(item,"copyinputs"):
            item.copyinputs(self)
        # else -- do nothing

    def output(self,item):
        if isinstance(item, str):
            itemnorm = "./"+os.path.normpath(item).replace("\\","/")
            self.script_list[1] += r"python.exe %AZ_BATCH_JOB_PREP_WORKING_DIR%\blobxfer.py --storageaccountkey {} --upload {} {} {} --remoteresource {}{}".format(self.storage_key,self.storage_account,self.datablob_fn, ".", itemnorm, os.linesep)
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

    result = map_reduce(range(15),
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

        job = batchmodels.JobAddParameter(id=job_id, pool_info=batch.models.PoolInformation(pool_id=self.poold_id))
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

        runner = AzureBatch(task_count=20,min_node_count=2,max_node_count=7,pool_id="twoa2x2") #!!!cmk there is a default core limit of 99
        #runner = LocalMultiProc(2)
        test_fun(runner)


# When there is an error, say so, don't just return the result from the previous good run
# Copy 2+ python path to the machines
# Auto config
#   auto upload python zip file
#   create pool for scratch or use an existing one
# make version and "mapreduce" a param
# Understand HDFS and Azure storage
# control the default core limit so can have more than taskcount=99
# Need share access Pool if want nodes to talk to each other?
# is 'datetime.timedelta(minutes=25)' right?

# DONE:
# Stop using fastlmm2 for storage
# Can run multiple jobs at once and they don't clobber each other
# Remove C:\user\tasks\ from code and use an enviornment variable instead
# Should shared inputfiles be in shared?
# make sure every use of storage: locally, in blobs, and on nodes, is sensible
# If multiple jobs run on the same machine, only download the data once.
# replace AzureBatchCopier("inputfiles" with something more automatic, based on local files

# DONE Copy input files to the machines
# DONE copy output files from the machine
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
