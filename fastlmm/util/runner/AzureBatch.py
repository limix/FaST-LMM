import logging
import datetime
import fastlmm.util.runner.azurehelper as commonhelpers #!!!cmk is this the best way to include the code from the Azure python sample's common.helper.py?
import os

try:
    import azure.batch.batch_service_client as batch 
    import azure.batch.batch_auth as batchauth 
    import azure.batch.models as batchmodels
except:
    pass

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Hello")

    job_id = commonhelpers.generate_unique_resource_name("HelloWorld")

    #Expect:
    # batch service url, e.g., https://fastlmm.westus.batch.azure.com
    # account, e.g., fastlmm
    # key, e.g. Wsz....

    batch_service_url, account, key = [s.strip() for s in open(os.path.expanduser("~")+"/azurebatch/cred.txt").xreadlines()]
    
    credentials = batchauth.SharedKeyCredentials(account, key)


    batch_client = batch.BatchServiceClient(credentials,base_url=batch_service_url)

    job = batchmodels.JobAddParameter(id=job_id, pool_info=batch.models.PoolInformation(pool_id="twoa1"))
    batch_client.job.add(job)

    # see http://azure-sdk-for-python.readthedocs.io/en/latest/_modules/azure/batch/models/task_add_parameter.html
    task = batchmodels.TaskAddParameter(
        id="HelloWorld",
        run_elevated=True,
        command_line=r"c:\Anaconda2\python.exe c:\user\tasks\shared\test.py"
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

# Install Python manully on both machines and then run a python cmd on the machines
# copy python program to machine and run it
# get iDistribute working on just two machines with pytnon already installed and no input files, just seralized input and seralized output
# Run python w/o needing to install it on machine
# Copy files to the machines
# Copy python path to the machines
# Understand HDFS and Azure storage
# more than 2 machines (grow)