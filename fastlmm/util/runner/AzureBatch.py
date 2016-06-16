import logging
import datetime
import fastlmm.util.runner.azurehelper as commonhelpers #!!!cmk is this the best way to include the code from the Azure python sample's common.helper.py?
import os

try:
    import azure.batch.batch_service_client as batch 
    import azure.batch.batch_auth as batchauth 
    import azure.batch.models as batchmodels
    import azure.storage.blob as azureblob

except Exception as exp:
    logging.warning(exp)
    pass

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Hello")

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

# copy python program to machine and run it
# get iDistribute working on just two machines with pytnon already installed and no input files, just seralized input and seralized output
# Run python w/o needing to install it on machine
# Copy files to the machines
# Copy python path to the machines
# Understand HDFS and Azure storage
# more than 2 machines (grow)

# DONE Install Python manully on both machines and then run a python cmd on the machines
