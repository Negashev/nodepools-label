import asyncio
import os
import re

from kubernetes_asyncio import client, config, watch

NODEPOOLS = {}
NODEPOOL_LABEL = os.getenv('NODEPOOL_LABEL', 'cattle.io/nodepool')


async def set_label(obj, hostnamePrefix):
    global NODEPOOL_LABEL
    if NODEPOOL_LABEL in obj['status']['nodeLabels']:
        if obj['status']['nodeLabels'][NODEPOOL_LABEL] == hostnamePrefix:
            return
    del obj['status']
    del obj['metadata']['creationTimestamp']
    del obj['metadata']['resourceVersion']
    obj['metadata']['labels'][NODEPOOL_LABEL] = hostnamePrefix
    v1 = client.CoreV1Api()
    # switch to cluster
    k8s_clusters_url = v1.api_client.configuration.host.rsplit('/', 1)[0]
    v1.api_client.configuration.host = k8s_clusters_url + '/' + obj['metadata']['namespace']
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


def main():
    loop = asyncio.get_event_loop()

    # Load the kubeconfig file specified in the KUBECONFIG environment
    # variable, or fall back to `~/.kube/config`.
    loop.run_until_complete(config.load_incluster_config())
    loop.run_until_complete(simple_watch_nodepools())

    # Define the tasks to watch namespaces and pods.
    tasks = [
        asyncio.ensure_future(watch_nodes()),
        asyncio.ensure_future(watch_nodepools())
    ]

    # Push tasks into event loop.
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()


if __name__ == '__main__':
    main()
