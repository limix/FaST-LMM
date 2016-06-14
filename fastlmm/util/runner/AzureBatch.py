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

    task = batchmodels.TaskAddParameter(
        id="HelloWorld",
        command_line=commonhelpers.wrap_commands_in_shell(
            'windows', ['echo Hello world from the Batch Hello world sample!'])
    )
    batch_client.task.add_collection(job_id, [task])
 
    commonhelpers.wait_for_tasks_to_complete(batch_client, job_id, datetime.timedelta(minutes=25))
 
 
    tasks = batch_client.task.list(job_id) 
    task_ids = [task.id for task in tasks]
 
 
    commonhelpers.print_task_output(batch_client, job_id, task_ids)
