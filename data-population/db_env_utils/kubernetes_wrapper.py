"""
Utility functions for extracting information from deployed versions of
postgres database.


"""

import logging

import requests
from env_config import OCParams
from kr8s.objects import Pod
from kubernetes import client

requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning
)

LOGGER = logging.getLogger(__name__)


class KubeClient:
    """
    Class for interacting with kubernetes cluster, extracting specific information
    necessary for the data extraction process.

    """

    def __init__(self, ocparams: OCParams):
        self.ocparams = ocparams
        self.config = client.Configuration()
        self.config.host = self.ocparams.host
        self.config.verify_ssl = False
        self.config.api_key = {"authorization": "Bearer " + self.ocparams.token}
        self.api_client = client.ApiClient(self.config)
        self.api = client.CoreV1Api(self.api_client)
        self.pf = None  # contains the port forward object, so that it can be closed when complete

    def get_pods(self, filter_str: str, exclude_strs: str) -> list:
        """
        Get all pods in the namespace.

        Returns:
            list: list of pods in the namespace

        """

        LOGGER.debug("Listing pods with their IPs:")
        ret = self.api.list_namespaced_pod(namespace=self.ocparams.namespace)
        pods = []
        for i in ret.items:
            exclude = False
            for exclude_str in exclude_strs:
                if exclude_str in i.metadata.name:
                    exclude = True

            if filter_str.lower() in i.metadata.name and not exclude:
                pods.append(i)
            LOGGER.debug(
                "%s\t%s\t%s"
                % (i.status.pod_ip, i.metadata.namespace, i.metadata.name)
            )
            # LOGGER.debug(i)

        return pods

    def get_secrets(
        self,
        namespace: str,
        filter_str: str,
    ):
        """
        Get the secrets from the namespace.

        Returns:
            dict: dictionary of secrets

        """
        filtered_secrets = []
        secrets = self.api.list_namespaced_secret(namespace=namespace)
        for secret in secrets.items:
            if filter_str.lower() in secret.metadata.name.lower():
                filtered_secrets.append(secret)
            LOGGER.debug("secrets: %s", secret.metadata.name)
        return filtered_secrets

    def open_port_forward(
        self,
        pod_name: str,
        namespace: str,
        local_port: int,
        remote_port: int,
    ):
        """
        Create a port forward to the pod.

        Returns:
            None

        """
        LOGGER.debug("Opening port forward for the pod: %s", pod_name)
        pod = Pod.get(name=pod_name, namespace=namespace)
        self.pf = pod.portforward(
            remote_port=remote_port,
            local_port=local_port,
        )
        self.pf.start()  # Start the port forward in a background thread
        LOGGER.debug("Port forward started")

    def close_port_forward(self):
        LOGGER.debug("Closing database port forward")
        if self.pf:
            self.pf.stop()
