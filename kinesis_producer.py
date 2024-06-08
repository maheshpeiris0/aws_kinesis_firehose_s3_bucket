import random
import time
import json
import boto3



def generate_message():
    """ Generate the message data with random values. """
    return {
        "event_type": "A",
        "symbol": random.choice(["PLUG", "AAPL", "TSLA", "MSFT", "AMZN", "GOOGL", "FB", "NVDA", "PYPL", "INTC", "CSCO", "CMCSA", "PEP", "ADBE", "NFLX", "AVGO", "TXN", "COST", "QCOM", "TMUS"]),
        "volume": random.randint(50, 1500),
        "accumulated_volume": random.randint(1000000, 50000000),
        "official_open_price": round(random.uniform(1, 5), 2),
        "vwap": round(random.uniform(1, 5), 2),
        "open": round(random.uniform(1, 10), 2),
        "close": round(random.uniform(1, 10), 2),
        "high": round(random.uniform(1, 12), 2),
        "low": round(random.uniform(1, 8), 2),
        "aggregate_vwap": round(random.uniform(1, 5), 4),
        "average_size": random.randint(50, 150),
        "start_timestamp": int(time.time() * 1000),
        "end_timestamp": int(time.time() * 1000) + 1000,
        "otc": None
    }
    
    
    
def kinesis_pipeline():
    
    kinesis = boto3.client('kinesis', region_name='us-east-1')
    
    while True:
        message = generate_message()
        message_str = json.dumps(message)
        print(f'Producing message: {message_str}')
        kinesis.put_record(
            StreamName='test',
            Data=message_str,
            PartitionKey=message['symbol']
        )
        time.sleep(0.2)



if __name__ == '__main__':
    kinesis_pipeline()
