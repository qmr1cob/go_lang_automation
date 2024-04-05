import os
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn
import asyncio
from xorder_operations import xorder_init

#Set proxy environment variables
os.environ["http_proxy"] = "http://10.143.16.65:8080"
os.environ["https_proxy"] = "http://10.143.16.65:8080"
os.environ["HTTP_PROXY"] = "http://10.143.16.65:8080"
os.environ["HTTPS_PROXY"] ="http://10.143.16.65:8080"


app = FastAPI()

@app.get('/')
def index():
    return {'data': 'Backend is running'}


@app.post("/post/order_info")
async def xorder_request(request: Request):
    # Read the request body as JSON
    body = await request.json()
    print("Request Body:", body)
    await xorder_init(body['order_id'], body['order_value'])
    # Prepare the response
    response_data = {"msg": "The order has been placed"}
    return JSONResponse(content=response_data)


if __name__ == "__main__":
    uvicorn.run(app, host='0.0.0.0', port=8080)
