import asyncio
import json
import time
import aiohttp
import sys

consumer_name=sys.argv[1]
consumer_group_name=sys.argv[2]
topic=sys.argv[3]
rate=sys.argv[4]

async def register_consumer_group():
    print("registering consumer group with name "+consumer_group_name)
    data={'consumer_group_name':consumer_group_name}
    async with aiohttp.ClientSession() as session:
        async with session.post('http://localhost:8080/consumer-group/add', json=data) as response:
            if response.status == 200:
                print("consumer group PUT Request Successful")
            else:
                print("consumer group PUT Request Failed")
async def register_consumer():
    print("registering consumer with name "+consumer_name)
    data={'consumer_group_name':consumer_group_name,'consumer_name':consumer_name}
    async with aiohttp.ClientSession() as session:
        async with session.post('http://localhost:8080/consumer/register-consumer', json=data) as response:
            if response.status == 200:
                print("consumer PUT Request Successful")
            else:
                print("consumer PUT Request Failed")
async def subscribe_topic():
    print("subscribing consumer with name "+consumer_name+" to topic"+topic)
    data={'consumer_name':consumer_name,'topic_list':topic}
    async with aiohttp.ClientSession() as session:
        async with session.post('http://localhost:8080/consumer/subscribe-topic', json=data) as response:
            if response.status == 200:
                print("consumer subsribe PUT Request Successful")
            else:
                print("consumer subscribe PUT Request Failed")

async def send_messages():
    await register_consumer_group()
    await register_consumer()
    await subscribe_topic()
    semaphore = asyncio.Semaphore(1)
    async with aiohttp.ClientSession() as session:
        data={'consumer_name':consumer_name}
        while True:
            async with semaphore:
                async with session.post('http://localhost:8080/consumer/poll', json=data) as response:
                    if response.status == 200:
                        print("consumer poll PUT Request Successful")
                        response_data = await response.json()
                        print(json.dumps(response_data, indent=4))
                        message_id=response_data.get('messageId')
                        print("acknowledging the message")
                        ack_data={'consumer_name':consumer_name,'message_id':message_id}
                        async with session.post('http://localhost:8080/consumer/acknowledgement', json=ack_data) as response_ack:
                            if response_ack.status==200:
                                ack_response_data=await response_ack
                                print("acknowledgement passed for message id"+str(message_id))
                            else:
                                print("acknowledgement failed for message id"+str(message_id))
                    else:
                        print("consumer poll Request Failed")

            # Wait for some time before sending the next message
            await asyncio.sleep(int(rate))  # Change the time interval as needed

asyncio.get_event_loop().run_until_complete(send_messages())
