import asyncio
from websocket import create_connection
import json
import time
import aiohttp
import stomper
import sys

topic=sys.argv[1]
rate=sys.argv[2]

async def startup_function():
    # Implement your startup logic here
    print("Startup function executed.")
    data={'topicName':topic}
    async with aiohttp.ClientSession() as session:
         async with session.post('http://localhost:8080/topic/add-topic', json=data) as response:
            if response.status == 200:
                print("PUT Request Successful")
            else:
                print("PUT Request Failed")

async def send_messages():
    await startup_function()

    ws = create_connection("ws://localhost:8080/producer")
    messages=[' from John',' from mark',' from clark',' from bruce',' from martha']
    i=0
    while True:
        message_id = str(int(time.time()))  # Generate a unique message ID
        message_type = ""
        message = topic+messages[i]
        i=(i+1)%len(messages)
        data = {'messageID': message_id, 'key': message_type, 'message': message}
        address="/publish/"+topic
        stomp_message = stomper.send(address, json.dumps(data))
        print(stomp_message)
        ws.send(stomp_message)
        print(f"Message sent: {data}")

        # Wait for some time before sending the next message
        await asyncio.sleep(int(rate))  # Change the time interval as needed

asyncio.get_event_loop().run_until_complete(send_messages())
