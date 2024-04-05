from pymongo import MongoClient
from schema import create_payload_tumbling_window_trigger, create_payload_schedule_trigger

async def create_mongo_client(connection_string, database_name):
    try:
        mongo_client = MongoClient(connection_string)
        database_client = mongo_client[database_name]
        return mongo_client, database_client
    except Exception as e:
        print(f"Error creating MongoDB client: {str(e)}")
        exit(1)

async def close_mongo_client(mongo_client):
    try:
        mongo_client.close()
    except Exception as e:
        print(f"Error closing MongoDB connection: {str(e)}")


async def insert_trigger_document(collection, trigger, xorderid, Status, formatted_datetime,RunId, message):
    try:
        # Create the new document based on the trigger type and parameters
        if trigger.properties.type == 'TumblingWindowTrigger':
            document = await create_payload_tumbling_window_trigger(trigger,xorderid,Status,formatted_datetime,RunId,message)
        else:
            document = await create_payload_schedule_trigger(trigger,xorderid,Status,formatted_datetime,RunId,message)

        # Check if a document with the same TriggerName and OrderID exists
        # print("OrderID", xorderid, "TriggerName", trigger.name)
        existing_doc = collection.find_one({"Xorder_id": document["Xorder_id"], "TriggerName": document["TriggerName"]})
        
        # If an existing document is found, update it
        if existing_doc:
            # Only update if there are changes in Status, Annotations, or Date_time
            if existing_doc.get("Status") != Status or \
               existing_doc.get("Annotations") != document.get("Annotations") or \
               existing_doc.get("Date_time") != formatted_datetime:
                # Update the existing document with the new information
                collection.update_one({"_id": existing_doc["_id"]}, {"$set": {
                    "Status": Status,
                    "Annotations": document.get("Annotations"),
                    "Date_time": formatted_datetime,
                    "RunId":  RunId,
                    "message":message
                }})
                print(f"Updated document with _id: {existing_doc['_id']}")
            else:
                print(f"No changes detected for document with _id: {existing_doc['_id']}")
        else:
            # If no existing document matches, insert the new document
            collection.insert_one(document)
            print(f"Inserted new document for Trigger: {document['TriggerName']} with OrderID: {xorderid}")

    except Exception as e:
        print(f"Error inserting/updating document into MongoDB: {str(e)}")
async def read_mongodb_collection_status(collection, trigger_name, xorderid):
    try:
        # Define the query to find documents with the specified trigger_name and xorderid
        query = {"TriggerName": trigger_name, "Xorder_id": xorderid}

        # Use the find() method to retrieve documents based on the query
        cursor = collection.find(query)

        # Iterate over the cursor to access each document
        for document in cursor:
            # Access the value of the desired key within each document
            value = document.get("Status")
            # Print the value of the key
            # print("read_mongodb_collection_status",value)
            # Return the value of the key
            return value
    except Exception as e:
        print(f"Error reading status from MongoDB: {str(e)}")
        return None

async def handle_multiple_requests(collection, trigger_name, xorderid):
    try:
        # Check if there is any document with the same TriggerName and Xorder_id
        existing_doc = collection.find_one({"Xorder_id": xorderid, "TriggerName": trigger_name})
        if existing_doc:
            print("Document with the same TriggerName and Xorder_id already exists.")
            return None
        else:
            # Check if any document with the specified trigger name exists
            trigger_document = collection.find_one({"TriggerName": trigger_name})
            if trigger_document:
                print("Trigger name exists")
                # If documents with the specified trigger name exist, perform further operations
                pipeline = [
    {"$match": {"TriggerName": trigger_name}},  # Match documents with the specified trigger_name
    {"$sort": {"_id": -1}},  # Sort by document ID in descending order (latest first)
    {"$limit": 1}  # Limit to only the first document, which will be the latest based on sorting
]
                cursor = collection.aggregate(pipeline)
                print("Document latest from mongodb:",cursor)
                documents_found = False
                latest_status = None
                # Check if any documents were found
                for document in cursor:
                    print("Document latest from mongodb:",document)
                    documents_found = True
                    latest_status = document.get("Status")
                    runid=document.get("RunId")
                    Xorder_id= document.get("Xorder_id")
                    break
                if not documents_found:
                    print("No documents found for the specified trigger name.")
                return latest_status, runid,Xorder_id
            else:
                print("Trigger name does not exist.")
                
                return "Not exist"
    except Exception as e:
        print(f"Error reading handle_multiple requests status from MongoDB: {str(e)}")
        return None
async def update_trigger_document_status(collection, trigger_name, xorderid, new_status,formatted_datetime):
    try:
        # Find the document to update based on trigger name and order ID
        query = {"TriggerName": trigger_name, "Xorder_id": xorderid}
        existing_document = collection.find_one(query)

        if existing_document:
            # Update the status field of the existing document
            update =collection.update_one(query, {"$set": {"Status": new_status,"Date_time":formatted_datetime}})
            print("update:",update)
            print("Document updated successfully.")
        else:
            print("Document not found for the specified trigger name and order ID.")
    except Exception as e:
        print(f"Error updating document status: {str(e)}")
