import json
import asyncio
import json
import os

from aio_pika import connect_robust, ExchangeType, Message, DeliveryMode


async def main(loop):
    connection, exchange = await setup_rabbitmq(loop)
    await publish_events(exchange)
    await connection.close()

async def setup_rabbitmq(loop):
    connection = await connect_robust(host=os.environ.get('RABBIT_HOST'),
                               login=os.environ.get('RABBIT_USER'),
                               password=os.environ.get('RABBIT_PASS'),
                               loop=loop
                               )
    # Creating a channel
    channel = await connection.channel()
    exchange = await channel.declare_exchange(
        "meds", ExchangeType.FANOUT
    )
    # auto_delete deletes the queue after it is consumed, and consumer disconnects:
    queue = await channel.declare_queue(name='events', auto_delete=True) 

    # FANOUT ignores the routing_key="new.events" (but we name the exchange anyway because)
    await queue.bind(exchange, "new.events")
    return connection, exchange


async def publish_events(exchange):
    with open("events.json") as f:
        events = json.load(f)
        for event in events:
            message = Message(
                json.dumps(event).encode(),
                delivery_mode=DeliveryMode.PERSISTENT
            )
            await exchange.publish(message, routing_key="new.events")
        print("pushed all events to RabbitMQ")


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
