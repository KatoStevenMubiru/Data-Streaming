import json
import requests

REST_PROXY_URL = "http://localhost:8082"

def get_topics():
    """Gets topics from REST Proxy"""
    url = f"{REST_PROXY_URL}/topics"  # TODO
    resp = requests.get(url)

    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError:
        print(f"Failed to get topics {json.dumps(resp.json(), indent=2)})")
        return []

    print("Fetched topics from Kafka:")
    print(json.dumps(resp.json(), indent=2))
    return resp.json()

def get_topic(topic_name):
    """Get specific details on a topic"""
    url = f"{REST_PROXY_URL}/topics/{topic_name}"  # TODO
    resp = requests.get(url)

    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError:
        print(f"Failed to get topic {topic_name}: {json.dumps(resp.json(), indent=2)}")

    print(f"Fetched details for topic {topic_name}:")
    print(json.dumps(resp.json(), indent=2))

def get_brokers():
    """Gets broker information"""
    url = f"{REST_PROXY_URL}/brokers"  # TODO
    resp = requests.get(url)

    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError:
        print(f"Failed to get brokers: {json.dumps(resp.json(), indent=2)}")

    print("Fetched brokers from Kafka:")
    print(json.dumps(resp.json(), indent=2))

def get_partitions(topic_name):
    """Prints partition information for a topic"""
    url = f"{REST_PROXY_URL}/topics/{topic_name}/partitions"  # TODO
    resp = requests.get(url)

    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError:
        print(f"Failed to get partitions for {topic_name}: {json.dumps(resp.json(), indent=2)}")

    print(f"Partitions for topic {topic_name}:")
    print(json.dumps(resp.json(), indent=2))

if __name__ == "__main__":
    topics = get_topics()
    if topics:
        get_topic(topics[0])
        get_brokers()
        if topics:
            get_partitions(topics[-1])
