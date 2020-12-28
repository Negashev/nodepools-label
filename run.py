import asyncio
import os
import re
import argparse
import datetime

from kubernetes_asyncio import client, config, watch
from kubernetes_asyncio.config.kube_config import FileOrData


parser = argparse.ArgumentParser()
parser.add_argument("-p", "--preemptible", default=False, action='store_true',
                    help="set label PREEMPTIBLE_LABEL with taint")

NODEPOOLS = {}
CLUSTERS = {}
NODEPOOL_LABEL = os.getenv('NODEPOOL_LABEL', 'cattle.io/nodepool')

FILTER_PREEMPTIBLE_LABEL = os.getenv('FILTER_PREEMPTIBLE_LABEL', 'prepare-preemptible')
WAIT_TIME_PREEMPTIBLE_LABEL = int(os.getenv('WAIT_TIME_PREEMPTIBLE_LABEL', '23'))
PREEMPTIBLE_LABEL = os.getenv('PREEMPTIBLE_LABEL', 'preemptible')


async def set_label(obj, hostnamePrefix, preemptible):
    global NODEPOOL_LABEL
    global CLUSTERS
    body = {
        "metadata": {
            "labels": {
                NODEPOOL_LABEL: hostnamePrefix
            }
        }
    }
    if preemptible:
        # filter node if prepare-preemptible label not exist
        if FILTER_PREEMPTIBLE_LABEL not in obj['status']['nodeLabels']:
            return
        if PREEMPTIBLE_LABEL in obj['status']['nodeLabels']:
            return
        # add PREEMPTIBLE_LABEL if time WAIT_TIME_PREEMPTIBLE_LABEL
        calculate_time = datetime.datetime.strptime(obj['metadata']['creationTimestamp'],"%Y-%m-%dT%H:%M:%SZ") + datetime.timedelta(hours = WAIT_TIME_PREEMPTIBLE_LABEL)
        if calculate_time < datetime.datetime.now():
            body = {
                "metadata": {
                    "labels": {
                        NODEPOOL_LABEL: hostnamePrefix,
                        PREEMPTIBLE_LABEL: "true"
                    }
                },
                "spec": {
                    "taints": [
                                  {
                                      "effect": "NoSchedule",
                                      "key": datetime.datetime.now().strftime('%s'),
                                      "value": "preemptible"
                                  }
                              ]
                }                 
            }
            print(f"set {PREEMPTIBLE_LABEL}=true for node {obj['spec']['requestedHostname']}")
    else:
        if NODEPOOL_LABEL in obj['status']['nodeLabels']:
            if obj['status']['nodeLabels'][NODEPOOL_LABEL] == hostnamePrefix:
                return         
    print(f"set {NODEPOOL_LABEL}={hostnamePrefix} for node {obj['spec']['requestedHostname']}")
    # connect to cluster for this node
    configuration = client.Configuration()
    configuration.host = CLUSTERS[obj['metadata']['namespace']]['apiEndpoint']
    configuration.ssl_ca_cert = FileOrData({'certificate-authority': CLUSTERS[obj['metadata']['namespace']]['caCert']},
                                           'certificate-authority', data_key_name='certificate-authority').as_file()
    configuration.api_key = {"authorization": "Bearer " + CLUSTERS[obj['metadata']['namespace']]['serviceAccountToken']}
    client.Configuration.set_default(configuration)
    v1 = client.CoreV1Api()
    await v1.patch_node(obj['spec']['requestedHostname'], body, _request_timeout=30)


async def simple_watch_nodepools():
    global NODEPOOLS
    this_nodepools = {}
    v1 = client.CustomObjectsApi()
    async with watch.Watch().stream(v1.list_cluster_custom_object, "management.cattle.io", "v3", "nodepools",
                                    timeout_seconds=10) as stream:
        async for event in stream:
            evt, obj = event['type'], event['object']
            nodepool_id = f"{obj['metadata']['namespace']}:{obj['metadata']['name']}"
            hostnamePrefix = re.sub(r'([-_.])$', '', obj['spec']['hostnamePrefix'])
            this_nodepools[nodepool_id] = hostnamePrefix
            NODEPOOLS[nodepool_id] = hostnamePrefix
    NODEPOOLS = this_nodepools


async def simple_watch_clusters():
    global CLUSTERS
    this_clusters = {}
    v1 = client.CustomObjectsApi()
    async with watch.Watch().stream(v1.list_cluster_custom_object, "management.cattle.io", "v3", "clusters",
                                    timeout_seconds=10) as stream:
        async for event in stream:
            evt, obj = event['type'], event['object']
            if obj['metadata']['name'] == 'local':
                continue
            cluster_id = obj['metadata']['name']
            try:
                credentials = {
                    'apiEndpoint': obj['status']['apiEndpoint'],
                    'caCert': obj['status']['caCert'],
                    'serviceAccountToken': obj['status']['serviceAccountToken'],
                }
                this_clusters[cluster_id] = credentials
                CLUSTERS[cluster_id] = credentials
            except Exception as e:
                print(f"Wait cluster {cluster_id}")
    CLUSTERS = this_clusters


async def simple_watch_nodes(preemptible=False):
    global NODEPOOLS
    v1 = client.CustomObjectsApi()
    async with watch.Watch().stream(v1.list_cluster_custom_object, "management.cattle.io", "v3", "nodes",
                                    timeout_seconds=10) as stream:
        async for event in stream:
            evt, obj = event['type'], event['object']
            if obj['spec']['nodePoolName'] and obj['spec']['nodePoolName'] in NODEPOOLS and evt in ["ADDED",
                                                                                                    "MODIFIED"]:
                try:
                    await set_label(obj, NODEPOOLS[obj['spec']['nodePoolName']], preemptible)
                except Exception as e:
                    # can't wait
                    print(f"Wait pool {obj['spec']['nodePoolName']}")




def main():
    args = parser.parse_args()
    loop = asyncio.get_event_loop()

    # Load the kubeconfig file specified in the KUBECONFIG environment
    # variable, or fall back to `~/.kube/config`.
    config.load_incluster_config()
    # loop.run_until_complete(config.load_kube_config())
    loop.run_until_complete(simple_watch_clusters())
    loop.run_until_complete(simple_watch_nodepools())
    loop.run_until_complete(simple_watch_nodes(args.preemptible))

    loop.close()


if __name__ == '__main__':
    main()
