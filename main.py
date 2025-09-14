import logging
from collections import defaultdict
from datetime import timedelta
from time import sleep
from typing import Any

import yaml
from fastapi import FastAPI
from infrahub_sdk import Config, InfrahubClientSync
from pygnmi.client import gNMIclient
from temporal_boost import BoostApp
from temporalio import activity, workflow
from temporalio.client import Client, WorkflowHandle

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
TEMPORAL_SERVER = "temporal:7233"
DEPLOYED_STORAGE_IDS = set()

app = BoostApp(
    name="BoostApp example",
    temporal_endpoint=TEMPORAL_SERVER,
    temporal_namespace="default",
    use_pydantic=True,
)

client = InfrahubClientSync(config=Config(address="http://host.docker.internal:8000"))

all_artifacts_query = """
query ArtifactQuery {
CoreArtifact {
    edges {
        node {
        checksum {
            value
        }
        storage_id {
            value
        }
        object {
            node {
            display_label
            id
            }
        }
        }
    }
    }
}
"""

artifact_query = """
query DeviceArtifactQuery($device: String!) {
InfraDevice(name__value: $device) {
    edges {
    node {
        name {
        value
        }
        artifacts {
        edges {
            node {
            checksum {
                value
            }
            storage_id {
                value
            }
            }
        }
        }
    }
    }
}
}
"""

device_query = """
query DeviceQuery($device_id: [ID]) {
InfraDevice(ids: $device_id) {
    edges {
    node {
        id
        name {
        value
        }
    }
    }
}
}
"""


@activity.defn(name="get_updated_devices")
async def get_updated_devices(branch: str) -> dict[str, str]:
    data_from_branch = client.execute_graphql(query=all_artifacts_query, branch_name=branch)
    data_from_main = client.execute_graphql(query=all_artifacts_query)
    devices: dict[tuple[str, str], str] = {}

    branch_artifacts: set[tuple[str, str, str, str]] = {
        (
            artifact["node"]["object"]["node"]["id"],
            artifact["node"]["checksum"]["value"],
            artifact["node"]["object"]["node"]["display_label"],
            artifact["node"]["storage_id"]["value"],
        )
        for artifact in data_from_branch["CoreArtifact"]["edges"]
    }
    main_artifacts: set[tuple[str, str, str, str]] = {
        (
            artifact["node"]["object"]["node"]["id"],
            artifact["node"]["checksum"]["value"],
            artifact["node"]["object"]["node"]["display_label"],
            artifact["node"]["storage_id"]["value"],
        )
        for artifact in data_from_main["CoreArtifact"]["edges"]
    }
    devices = {(artifact[0], artifact[1]): artifact[2] for artifact in branch_artifacts.difference(main_artifacts)}

    return devices


@activity.defn(name="get_artifact")
async def get_artifact(device: str, expected_checksum: str) -> dict[str, Any]:
    artifact_rendered = False

    while not artifact_rendered:
        sleep(3)
        data = client.execute_graphql(query=artifact_query, variables={"device": device})
        checksum = data["InfraDevice"]["edges"][0]["node"]["artifacts"]["edges"][0]["node"]["checksum"]["value"]
        storage_id = data["InfraDevice"]["edges"][0]["node"]["artifacts"]["edges"][0]["node"]["storage_id"]["value"]
        artifact_rendered = True if checksum == expected_checksum else False

    response = client.object_store.get(identifier=storage_id)
    artifact = yaml.load(response, Loader=yaml.SafeLoader)
    return artifact


@activity.defn(name="get_device")
async def get_device(device_id: str) -> str:
    data = client.execute_graphql(query=device_query, variables={"device_id": device_id})
    return data["InfraDevice"]["edges"][0]["node"]["name"]["value"]


@activity.defn(name="configure_device")
async def configure_device(device: str, artifact: dict[str, Any]) -> dict[str, dict[str, list[str]]]:
    """
    Main function to connect to a gNMI-enabled device and send a Set RPC with an update action.
    """
    diffs: dict[str, list[str]] = defaultdict(list)

    # --- Device Connection Details ---
    device_port = 57400
    username = "admin"
    password = "NokiaSrl1!"

    print(f"Connecting to {device}:{device_port}...")

    try:
        # --- Establish gNMI Connection ---
        with gNMIclient(
            target=(device, device_port),
            username=username,
            password=password,
            insecure=False,
            skip_verify=True,
            show_diff="get",
            encoding="json_ietf",
        ) as client:
            print("Successfully connected to the device.")

            gnmi_response = client.set(replace=[("/", artifact)])

            # --- Process and Display the Response ---
            # A successful Set RPC will return a response with a timestamp.
            # The response object can be iterated to check individual operation results.
            print("\n--- gNMI SetResponse ---")
            print(gnmi_response)
            print("------------------------\n")
            print("Configuration replace sent successfully.")

            if isinstance(gnmi_response, tuple):
                for diff in gnmi_response[1]:
                    diffs[diff[1]].append(f"{diff[0]} {diff[2]}")
            else:
                diffs["/"].append("NO CHANGES")

    except Exception as e:
        print(f"\nAn error occurred: {e}")
        raise Exception(f"An error occurred: {e}")

    return {device: diffs}


@workflow.defn(sandboxed=False, name="ProposedChangeWorkflow")
class ProposedChangeWorkflow:
    def __init__(self) -> None:
        self.generated_artifacts: set[str] = set()
        self.received_signals = []

    @workflow.run
    async def run(self, payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
        from temporalio.workflow import asyncio

        devices = await workflow.execute_activity(
            activity=get_updated_devices,
            arg=payload["branch"],
            schedule_to_close_timeout=timedelta(seconds=300),
            start_to_close_timeout=timedelta(seconds=60),
        )

        await workflow.wait_condition(lambda: set(devices.keys()).issubset(self.generated_artifacts))

        child_promises = []
        for target_checksum, device in devices.items():
            expected_checksum = target_checksum.split(",")[1]
            child_promises.append(
                workflow.execute_child_workflow(
                    workflow=ConfigureDevice.run,
                    args=[device, expected_checksum],
                    id=f"{device}-proposed-change-{payload['data']['proposed_change_id']}",
                    task_queue="default_queue",
                )
            )
        results = await asyncio.gather(*child_promises)
        workflow.logger.info("Child workflows finished. Results: %s", results)
        return results

    @workflow.signal
    async def update_generated_artifacts(self, target_id: str, checksum: str) -> None:
        self.generated_artifacts.add(f"{target_id},{checksum}")
        self.received_signals.append(f"Added generated artifacts: {target_id=}, {checksum=}")


@workflow.defn(sandboxed=False, name="PortkeyWorkflow")
class PortkeyWorkflow:
    @workflow.run
    async def run(self, payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
        device = await workflow.execute_activity(
            activity=get_device,
            arg=payload["data"]["target_id"],
            schedule_to_close_timeout=timedelta(seconds=300),
            start_to_close_timeout=timedelta(seconds=60),
        )

        results = await workflow.execute_child_workflow(
            workflow=ConfigureDevice.run,
            args=[device, payload["data"]["checksum"]],
            id=f"{device}-portkey-{payload['data']['target_id']}",
            task_queue="default_queue",
        )

        workflow.logger.info("Child workflow finished. Results: %s", results)
        return results


@workflow.defn(sandboxed=False, name="ConfigureDeviceWorkflow")
class ConfigureDevice:
    @workflow.run
    async def run(self, device: str, expected_checksum: str) -> dict[str, Any]:
        artifact = await workflow.execute_activity(
            activity=get_artifact,
            args=[device, expected_checksum],
            schedule_to_close_timeout=timedelta(seconds=300),
            start_to_close_timeout=timedelta(seconds=60),
        )
        diffs = await workflow.execute_activity(
            activity=configure_device,
            args=[device, artifact],
            schedule_to_close_timeout=timedelta(seconds=300),
            start_to_close_timeout=timedelta(seconds=60),
        )
        return diffs


app.add_worker(
    "default_worker",
    "default_queue",
    workflows=[ProposedChangeWorkflow, PortkeyWorkflow, ConfigureDevice],
    activities=[get_updated_devices, get_artifact, configure_device, get_device],
    max_concurrent_activities=20,
    max_concurrent_workflow_tasks=20,
)


async def run_proposed_change_workflow(payload: dict[str, Any]) -> WorkflowHandle:
    client = await Client.connect(TEMPORAL_SERVER, namespace="default")

    # Use a unique ID for each workflow execution to allow concurrent runs.
    workflow_id = f"proposed-change-{payload['data']['proposed_change_id']}"
    return await client.start_workflow(
        workflow=ProposedChangeWorkflow.run,
        arg=payload,
        # start_delay=timedelta(seconds=10),
        id=workflow_id,
        task_queue="default_queue",
    )


async def run_portkey_workflow(payload: dict[str, Any]) -> WorkflowHandle:
    client = await Client.connect(TEMPORAL_SERVER, namespace="default")

    # Use a unique ID for each workflow execution to allow concurrent runs.
    workflow_id = f"portkey-{payload['data']['target_id']}"
    return await client.start_workflow(
        workflow=PortkeyWorkflow.run,
        arg=payload,
        # start_delay=timedelta(seconds=10),
        id=workflow_id,
        task_queue="default_queue",
    )


async def signal_running_workflows(payload: dict[str, Any]) -> WorkflowHandle:
    client = await Client.connect(TEMPORAL_SERVER, namespace="default")

    query = "WorkflowType = 'ProposedChangeWorkflow' AND ExecutionStatus = 'Running'"
    running_workflow_ids = []

    async for workflow_execution_description in client.list_workflows(query):
        logger.info(
            f"Found running workflow: ID='{workflow_execution_description.id}', RunID='{workflow_execution_description.run_id}'"
        )
        running_workflow_ids.append(workflow_execution_description.id)

    if not running_workflow_ids:
        logger.info("No running ProposedChangeWorkflows found.")
        return

    for workflow_id in running_workflow_ids:
        workflow_handle = client.get_workflow_handle(workflow_id=workflow_id)
        await workflow_handle.signal(
            signal=ProposedChangeWorkflow.update_generated_artifacts,
            args=[payload["data"]["target_id"], payload["data"]["checksum"]],
        )


fastapi_app = FastAPI(docs_url="/doc")
app.add_asgi_worker("asgi_worker", fastapi_app, "0.0.0.0", 8001)


@fastapi_app.post("/proposed_change_workflow")
async def proposed_change_workflow(payload: dict[str, Any]) -> str:
    handle = await run_proposed_change_workflow(payload)
    return f"Workflow {handle.id} started."


@fastapi_app.post("/portkey_workflow")
async def portkey_workflow(payload: dict[str, Any]) -> str:
    handle = await run_portkey_workflow(payload)
    return f"Workflow {handle.id} started."


@fastapi_app.post("/artifact_updated")
async def artifact_updated(payload: dict[str, Any]) -> str:
    await signal_running_workflows(payload)
    return "Signalled all running workflows"


if __name__ == "__main__":
    app.run()
