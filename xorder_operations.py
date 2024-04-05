import asyncio
from azure.identity import ClientSecretCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from dotenv import load_dotenv
import os
import sys
from db_operations import insert_trigger_document, create_mongo_client, close_mongo_client, read_mongodb_collection_status, handle_multiple_requests, update_trigger_document_status
from schema import create_payload_tumbling_window_trigger, create_payload_schedule_trigger
from datetime import datetime, timedelta,timezone

load_dotenv()

# Azure Data Factory and MongoDB connection parameters
subscription_id = os.getenv("SUBSCRIPTION_ID")
resource_group_name = os.getenv("RESOURCE_GROUP_NAME")
data_factory_name = os.getenv("DATA_FACTORY_NAME")
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
tenant_id = os.getenv("TENANT_ID")
collection_name = "dataxpress-xorder-status"

# MongoDB connection parameters
mongo_connection_string = os.getenv("MONGO_CONNECTION_STRING")
current_datetime = datetime.now()
formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

async def authenticate_adf_client():
    try:
        credentials = ClientSecretCredential(client_id=client_id, client_secret=client_secret, tenant_id=tenant_id)
        adf_client = DataFactoryManagementClient(credentials, subscription_id)
        return adf_client
    except Exception as e:
        print(f"Error authenticating with Azure Data Factory: {str(e)}")
        return None

async def connect_to_mongodb():
    try:
        mongo_client, database_client = await create_mongo_client(mongo_connection_string, os.getenv("database_name"))
        return mongo_client, database_client
    except Exception as e:
        print(f"Error connecting to MongoDB: {str(e)}")
        return None, None

async def schedule_trigger_properties(properties_of_trigger):
    try:
        print("trigger_properties.pipelines", properties_of_trigger.pipelines)
        for pipeline_reference_name in properties_of_trigger.pipelines:
            print("Pipeline Reference Name:", pipeline_reference_name.pipeline_reference.reference_name)
            pipeline_name = pipeline_reference_name.pipeline_reference.reference_name
        return pipeline_name
    except Exception as e:
        print(f"Error in getting schedule_trigger_properties: {str(e)}")

async def tumbling_trigger_properties(properties_of_trigger):
    try:
        pipeline_name = properties_of_trigger.pipeline.pipeline_reference.reference_name
        print("pipeline_name:", pipeline_name)
        return pipeline_name
    except Exception as e:
        print(f"Error in getting tumbling_trigger_properties: {str(e)}")

async def pipeline_run(pipeline_name, para):
    try:
        adf_client = await authenticate_adf_client()
        response = adf_client.pipelines.create_run(
            resource_group_name=resource_group_name,
            factory_name=data_factory_name,
            pipeline_name=pipeline_name,
            parameters=para
        )
        print("response", response)
        return response.run_id
    except Exception as e:
        print(f"Error in getting pipeline_run: {str(e)}")

async def pipeline_run_status(pipeline_run_id):
    try:
        adf_client = await authenticate_adf_client()
        pipeline_run_status = adf_client.pipeline_runs.get(
            resource_group_name,
            data_factory_name,
            run_id=pipeline_run_id
        )
        print("Status of the pipeline run:", pipeline_run_status.status)
        return pipeline_run_status.status, pipeline_run_status.run_id
    except Exception as e:
        print(f"Error in getting pipeline_run_status: {str(e)}")

        

async def trigger_properties_ftn(trigger_name):
    try:
        adf_client = await authenticate_adf_client()
        print("trigger_check")
        triggers = adf_client.triggers.list_by_factory(resource_group_name, data_factory_name)
        for trigger in triggers:
            print(trigger.name)
        trigger_exists = any(trigger.name == trigger_name for trigger in triggers)
        print("Found triggers :", trigger_exists)
        if trigger_exists:
            print("Found triggers :", trigger_exists)
            trigger_properties = adf_client.triggers.get(resource_group_name, data_factory_name, trigger_name)
            return trigger_properties
        else:
            return "Not found trigger"
    except Exception as e:
        print(f"Error in getting trigger properties: {str(e)}")



async def trigger_run(trigger_name,database_client,xorderid):
    try:
        adf_client = await authenticate_adf_client()
        triggers = adf_client.triggers.list_by_factory(resource_group_name, data_factory_name)
        trigger_exists = any(trigger.name == trigger_name for trigger in triggers)
    except Exception as e:
        print(f"Error in trigger asynchronously: {str(e)}")

    if trigger_exists:
        print("Found triggers:", trigger_exists)
        try:
            trigger_properties = adf_client.triggers.get(resource_group_name, data_factory_name, trigger_name)
            try:
                await insert_trigger_document(database_client[collection_name], trigger_properties, xorderid, "Locked", formatted_datetime, "runid", "initial stage")
            except Exception as e:
                print(f"Error in insert trigger document: {str(e)}")
        except Exception as e:
            print(f"Error in trigger properties asynchronously: {str(e)}")
            print("Trigger parameter:", trigger_properties.properties.pipeline.parameters)
        print("Trigger parameter:", trigger_properties.properties.pipeline.parameters)
        trigger_type = trigger_properties.properties.type
        para = trigger_properties.properties.pipeline.parameters
        print("Trigger Type:", trigger_type)
        # pipeline_run_id="6c5a05bc-e5b5-11ee-83d6-00505682bbc0"
        try:
            if trigger_type == 'TumblingWindowTrigger':
                pipeline_name = await tumbling_trigger_properties(trigger_properties.properties)
                pipeline_run_id = await pipeline_run(pipeline_name, para)
            else:
                pipeline_name = await schedule_trigger_properties(trigger_properties.properties, trigger_properties.properties.pipelines.parameters)
                pipeline_run_id = await pipeline_run(pipeline_name, trigger_properties.properties.pipelines)
            initial_update_mongostatus = await read_mongodb_collection_status(database_client[collection_name], trigger_name, xorderid)
            print("The initial status of MongoDB for trigger name and xorderid:", initial_update_mongostatus)
            try:
                if initial_update_mongostatus not in ['Succeeded', 'Failed', 'Cancelled', 'Canceling']:
                    try:
                            Status, messages = await pipeline_run_status(pipeline_run_id)
                            print("Status, message", Status, messages)
                            await insert_trigger_document(database_client[collection_name], trigger_properties, xorderid, Status, formatted_datetime, messages, messages)
                            initial_update_mongostatus = await read_mongodb_collection_status(database_client[collection_name], trigger_name, xorderid)
                            
                    except Exception as e:
                            print(f"Error in updating initial_update_mongostatus: {str(e)}")
            except Exception as e:
                print(f"Error in looping for update: {str(e)}")

                print("Initial update MongoDB status:", initial_update_mongostatus)
        except Exception as e:
            print(f"Error in initial_update_mongostatus: {str(e)}")
        
    else:
        print("trigger run properties")    


async def get_latest_trigger_run_status(trigger_name):
    try:
        end_time = datetime.utcnow()  # Current time
        start_time = end_time - timedelta(days=1)
        adf_client = await authenticate_adf_client()
        if adf_client is None:
            return None
        # Create filter parameters
        filter_params = RunFilterParameters(
        last_updated_after=start_time,
        last_updated_before=end_time,
        filters=[
            RunQueryFilter(
                operand="TriggeredByName",
                operator="Equals",
                values=[trigger_name]
            )
        ],
        order_by=[
            RunQueryOrderBy(
                order_by="RunEnd",
                order="DESC"
            )])
        
        # Get the latest trigger run
        triggerruns= adf_client.pipeline_runs.query_by_factory(
        resource_group_name,
        data_factory_name,
        filter_parameters=filter_params)
    
        # print("trigger_runs::",pipelineruns.value[0])
        t= len(triggerruns.value)
        print("looping:",t)
        for i in triggerruns.value:
            # print(i)
            print("invoked_by.name:",i.invoked_by.name,"i.run_id",i.run_id,
            "last_updated",i.last_updated,
            "run_start",i.run_start,
            "status",i.status)
            status =i.status
        return status
    except Exception as e:
        print(f"Error in trigger run latest status from ADF: {str(e)}")


async def time_check_adf(trigger_name):
    try:
        trigger=await trigger_properties_ftn(trigger_name)
        print(trigger.properties)
        print(trigger.properties.frequency) #Hour
        print(trigger.properties.interval) #12 hr or 24hr
        print(trigger.properties.start_time)
           
    except Exception as e:
        print(f"Error in ADF properties of trigger: {str(e)}")        # Assuming start_time_str is a datetime.datetime object
       
    try:
            # Get the present time in UTC timezone
        start_time_str = trigger.properties.start_time
        present_time = datetime.now(timezone.utc) 
        print("present_time",present_time)

        
        # Strip date part and get time only
        start_time_time = start_time_str.time()
        present_time_time = present_time.time()
        print("start_time_time:", start_time_time)
        print("present_time_time:", present_time_time)

        # Convert time to seconds
        start_time_seconds = start_time_time.hour * 3600 + start_time_time.minute * 60 + start_time_time.second
        present_time_seconds = present_time_time.hour * 3600 + present_time_time.minute * 60 + present_time_time.second

        # Calculate the time difference in seconds
        time_difference_seconds = present_time_seconds - start_time_seconds
        time_difference_seconds = abs(time_difference_seconds)
        # Convert the time difference back to hours, minutes, and seconds
        # Calculate hours, minutes, and seconds
        hours = time_difference_seconds // 3600
        remaining_seconds = time_difference_seconds % 3600
        minutes = remaining_seconds // 60
        seconds = remaining_seconds % 60


        # Format the time difference as HH:MM:SS
        time_difference_str = "{:02}:{:02}:{:02}".format(hours, minutes, seconds)
        print("Time difference (HH:MM:SS):", time_difference_str)
        print("Time difference :", time_difference_seconds)
        

       
        if time_difference_seconds> 2 * 3600:
            print("The time difference exceeds 2 hours.")
        else:
            print("The time difference does not exceed 2 hours.")
        return time_difference_seconds
    except Exception as e:
        print(f"Error getting difference in timestamps: {str(e)}")
        return None 
async def xorder_init(xorderId,triggerName):
# async def main():
    try:
        adf_client = await authenticate_adf_client()
        print(adf_client)
        if adf_client is None:
            print("Error: Could not authenticate with Azure Data Factory.")
            return

        mongo_client, database_client = await connect_to_mongodb()
        if mongo_client is None or database_client is None:
            print("Error: Could not connect to MongoDB.")
            return
        
        trigger_name =triggerName
        print("Trigger name:", trigger_name)

        
        xorderid =xorderId
        print("Order Id is:", xorderid)

        try:
            trigger_property_time =await time_check_adf(trigger_name)
            print("trigger_property_time",trigger_property_time)
            if trigger_property_time > 2*3600:
                trigger_running_current_status_adf =await get_latest_trigger_run_status(trigger_name)
                print("Trigger running current status in adf result:",trigger_running_current_status_adf)
                try:
                    if trigger_running_current_status_adf in ['Succeeded', 'Failed', 'Cancelled', 'Canceling']:
                        handle_result = await handle_multiple_requests(database_client[collection_name], trigger_name, xorderid)
                        print("The previous trigger stautus in mongodb handle_result: ",handle_result)
                        if handle_result==None:
                            handling_result ="Already the same document exist"
                        elif handle_result == "Not exist":
                            await insert_trigger_document(database_client[collection_name], await trigger_properties_ftn(trigger_name), xorderid, None, formatted_datetime,None, "message")
                            await trigger_run(trigger_name,database_client,xorderid)
                            
                        else:
                            latest_status, runid_db,Xorder_id = handle_result
                            previous_RunStatus, messages = await pipeline_run_status(runid_db)
                            print(" Previous RunStatus of trigger , runid ", previous_RunStatus, messages )
                            if previous_RunStatus  in ['Succeeded', 'Failed', 'Cancelled', 'Canceling']:
                                status_updated= await trigger_run(trigger_name,database_client,xorderid)
                                await update_trigger_document_status(database_client[collection_name], trigger_name, Xorder_id, previous_RunStatus,formatted_datetime)
                                print("Trigger updated and added in mongodb.")
                        
                            else:
                                print("No updation for document since it is already running ")                        
                        
                            print("Trigger for the pipeline is already running")
                        return trigger_running_current_status_adf

                        await close_mongo_client(mongo_client)

                except Exception as e:
                    print(f"The trigger is running in ADF: {str(e)}")


                
                
                print("The time difference exceeds 2 hours.")
                return "Pipeline running status is {}"
            else:
                print("The time difference does not exceed 2 hours.")
            
        except Exception as e:
            print(f"Error getting difference in timestamps of ADF: {str(e)}")
            return None              

        
    except Exception as e:
        print(f"Error in main function: {str(e)}")

