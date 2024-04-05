
async def emptylist_handling(annotations_list):    
    return annotations_list[0] if annotations_list else 'no value'
async def create_payload_tumbling_window_trigger(trigger,xorderid,Status,formatted_datetime,RunId,message):
    properties = trigger.properties
    document ={}
    document ={
        "Xorder_id" :xorderid,
        "TriggerName": trigger.name,  
        "Status": Status,         
        "Annotations": await emptylist_handling(properties.annotations) ,
        "Date_time":formatted_datetime,
        "RunId":  RunId,      
        "message":message
    }
    print("document",document)
    return {
        "Xorder_id" :xorderid,
        "TriggerName": trigger.name,  
        "Status": Status,         
        "Annotations": await emptylist_handling(properties.annotations) ,
        "Date_time":formatted_datetime,   
        "RunId":  RunId,        
        "message":message
    }

async def create_payload_schedule_trigger(trigger,xorderid,Status,formatted_datetime,RunId,message):
    properties = trigger.properties
    return {
        "Xorder_id" :xorderid,
        "TriggerName": trigger.name,
        "Status": Status,       
        "Annotations": await emptylist_handling(properties.annotations),       
        "Date_time":formatted_datetime,        
        "message":message
    }
