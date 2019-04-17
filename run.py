import asyncio
import os
import re

from kubernetes_asyncio import client, config, watch
from kubernetes_asyncio.config.kube_config import FileOrData

NODEPOOLS = {}
CLUSTERS = {}
NODEPOOL_LABEL = os.getenv('NODEPOOL_LABEL', 'cattle.io/nodepool')


async def set_label(obj, hostnamePrefix):
    global NODEPOOL_LABEL
    global CLUSTERS
    if NODEPOOL_LABEL in obj['status']['nodeLabels']:
        if obj['status']['nodeLabels'][NODEPOOL_LABEL] == hostnamePrefix:
            return
    # connect to cluster for this node
    configuration = client.Configuration()
    configuration.host = CLUSTERS[obj['metadata']['namespace']]['apiEndpoint']
    configuration.ssl_ca_cert = FileOrData({'certificate-authority': CLUSTERS[obj['metadata']['namespace']]['caCert']},
                                           'certificate-authority', data_key_name='certificate-authority').as_file()
    configuration.api_key = {"authorization": "Bearer " + CLUSTERS[obj['metadata']['namespace']]['serviceAccountToken']}
    client.Configuration.set_default(configuration)
    v1 = client.CoreV1Api()
    body = {
        "metadata": {
            "labels": {
                NODEPOOL_LABEL: hostnamePrefix}
        }
    }
    print(f"set {NODEPOOL_LABEL}={hostnamePrefix} for node {obj['spec']['requestedHostname']}")
    await v1.patch_node(obj['spec']['requestedHostname'], body)


async def watch_nodes():
    global NODEPOOLS
    while True:
        v1 = client.CustomObjectsApi()
        async with watch.Watch().stream(v1.list_cluster_custom_object, "management.cattle.io", "v3", "nodes") as stream:
            async for event in stream:
                evt, obj = event['type'], event['object']
                if obj['spec']['nodePoolName'] and obj['spec']['nodePoolName'] in NODEPOOLS and evt in ["ADDED",
                                                                                                        "MODIFIED"]:
                    await set_label(obj, NODEPOOLS[obj['spec']['nodePoolName']])


async def watch_nodepools():
    global NODEPOOLS
    while True:
        this_nodepools = {}
        v1 = client.CustomObjectsApi()
        async with watch.Watch().stream(v1.list_cluster_custom_object, "management.cattle.io", "v3",
                                        "nodepools") as stream:
            async for event in stream:
                evt, obj = event['type'], event['object']
                nodepool_id = f"{obj['metadata']['namespace']}:{obj['metadata']['name']}"
                hostnamePrefix = re.sub(r'([-_.])$', '', obj['spec']['hostnamePrefix'])
                this_nodepools[nodepool_id] = hostnamePrefix
                NODEPOOLS[nodepool_id] = hostnamePrefix
        NODEPOOLS = this_nodepools


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


async def watch_clusters():
    global CLUSTERS
    while True:
        this_clusters = {}
        v1 = client.CustomObjectsApi()
        async with watch.Watch().stream(v1.list_cluster_custom_object, "management.cattle.io", "v3",
                                        "clusters") as stream:
            async for event in stream:
                evt, obj = event['type'], event['object']
                if obj['metadata']['name'] == 'local':
                    continue
                cluster_id = obj['metadata']['name']
                credentials = {
                    'apiEndpoint': obj['status']['apiEndpoint'],
                    'caCert': obj['status']['caCert'],
                    'serviceAccountToken': obj['status']['serviceAccountToken'],
                }
                this_clusters[cluster_id] = credentials
                CLUSTERS[cluster_id] = credentials
        CLUSTERS = this_clusters


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
            credentials = {
                'apiEndpoint': obj['status']['apiEndpoint'],
                'caCert': obj['status']['caCert'],
                'serviceAccountToken': obj['status']['serviceAccountToken'],
            }
            this_clusters[cluster_id] = credentials
            CLUSTERS[cluster_id] = credentials
    CLUSTERS = this_clusters


def main():
    loop = asyncio.get_event_loop()

    # Load the kubeconfig file specified in the KUBECONFIG environment
    # variable, or fall back to `~/.kube/config`.
    config.load_incluster_config()
    # loop.run_until_complete(config.load_kube_config())
    loop.run_until_complete(simple_watch_clusters())
    loop.run_until_complete(simple_watch_nodepools())

    # Define the tasks to watch namespaces and pods.
    tasks = [
        asyncio.ensure_future(watch_nodes()),
        asyncio.ensure_future(watch_nodepools()),
        asyncio.ensure_future(watch_clusters())
    ]

    # Push tasks into event loop.
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()


if __name__ == '__main__':
    main()
