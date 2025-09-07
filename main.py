import logging
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

import yaml
from fastapi import FastAPI
from infrahub_sdk import Config, InfrahubClientSync
from pygnmi.client import gNMIclient
from temporal_boost import BoostApp
from temporalio import activity, workflow
from temporalio.client import Client, WorkflowHandle

logging.basicConfig(level=logging.INFO)
TEMPORAL_SERVER = "temporal:7233"

app = BoostApp(
    name="BoostApp example",
    temporal_endpoint=TEMPORAL_SERVER,
    temporal_namespace="default",
    use_pydantic=True,
)

client = InfrahubClientSync(config=Config(address="http://infrahub-server-1:8000"))

all_artifacts_query = """query AllDevicesArtifactsQuery {
InfraDevice {
    edges {
    node {
        name {
        value
        }
        artifacts {
        edges {
            node {
            storage_id {
                value
                updated_at
            }
            }
        }
        }
    }
    }
}
}"""

artifact_query = """query DeviceArtifactQuery($device: String!) {
InfraDevice(name__value: $device) {
    edges {
    node {
        name {
        value
        }
        artifacts {
        edges {
            node {
            storage_id {
                value
                updated_at
            }
            }
        }
        }
    }
    }
}
}"""


@activity.defn(name="get_updated_devices")
async def get_updated_devices(occured_at: str) -> list[str]:
    data = client.execute_graphql(query=all_artifacts_query)
    devices: list[str] = []
    occured_at_format_code = "%Y-%m-%d %H:%M:%S.%f%z"
    updated_at_format_code = "%Y-%m-%dT%H:%M:%S.%f%z"

    for device in data["InfraDevice"]["edges"]:
        for artifact in device["node"]["artifacts"]["edges"]:
            updated_at = artifact["node"]["storage_id"]["updated_at"]
            updated_at_dt = datetime.strptime(updated_at, updated_at_format_code)
            occured_at_dt = datetime.strptime(occured_at, occured_at_format_code)

            if updated_at_dt >= occured_at_dt:
                devices.append(device["node"]["name"]["value"])

    return devices


@activity.defn(name="get_artifact")
async def get_artifact(device: str) -> dict[str, Any]:
    data = client.execute_graphql(query=artifact_query, variables={"device": device})
    artifact_id = data["InfraDevice"]["edges"][0]["node"]["artifacts"]["edges"][0]["node"]["storage_id"]["value"]
    response = client.object_store.get(identifier=artifact_id)
    artifact = yaml.load(response, Loader=yaml.SafeLoader)
    return artifact


@activity.defn(name="configure_device")
async def configure_device(device: str, artifact: dict[str, Any]) -> dict[str, list[str]]:
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

    return diffs


@workflow.defn(sandboxed=False, name="DeploymentWorkflow")
class Deployment:
    @workflow.run
    async def run(self, payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
        from temporalio.workflow import asyncio

        devices = await workflow.execute_activity(
            activity=get_updated_devices,
            arg=payload["occured_at"],
            schedule_to_close_timeout=timedelta(seconds=10),
        )

        child_promises = []
        for device in devices:
            child_promises.append(
                workflow.execute_child_workflow(
                    workflow=ConfigureDevice.run,
                    arg=device,
                    id=f"{device}-proposed-change-{payload['data']['proposed_change_id']}",
                    task_queue="default_queue",
                )
            )
        results = await asyncio.gather(*child_promises)
        workflow.logger.info("Child workflows finished. Results: %s", results)
        return results


@workflow.defn(sandboxed=False, name="ConfigureDeviceWorkflow")
class ConfigureDevice:
    @workflow.run
    async def run(self, device: str) -> dict[str, Any]:
        artifact = await workflow.execute_activity(
            activity=get_artifact,
            arg=device,
            schedule_to_close_timeout=timedelta(seconds=10),
        )
        diffs = await workflow.execute_activity(
            activity=configure_device,
            args=[device, artifact],
            schedule_to_close_timeout=timedelta(seconds=10),
        )
        return diffs


app.add_worker(
    "default_worker",
    "default_queue",
    workflows=[Deployment, ConfigureDevice],
    activities=[get_updated_devices, get_artifact, configure_device],
)


async def run_deployment_workflow(payload: dict[str, Any]) -> WorkflowHandle:
    client = await Client.connect(TEMPORAL_SERVER, namespace="default")

    # Use a unique ID for each workflow execution to allow concurrent runs.
    workflow_id = f"proposed-change-{payload['data']['proposed_change_id']}"
    return await client.start_workflow(
        workflow=Deployment.run,
        arg=payload,
        start_delay=timedelta(seconds=10),
        id=workflow_id,
        task_queue="default_queue",
    )


fastapi_app = FastAPI(docs_url="/doc")
app.add_asgi_worker("asgi_worker", fastapi_app, "0.0.0.0", 8001)


@fastapi_app.post("/deployment_workflow")
async def deployment_workflow(payload: dict[str, Any]) -> str:
    handle = await run_deployment_workflow(payload)
    return f"Workflow {handle.id} started."


if __name__ == "__main__":
    app.run()
