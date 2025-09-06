import logging
import uuid
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

app = BoostApp(
    name="BoostApp example",
    temporal_endpoint="temporal:7233",
    temporal_namespace="default",
    use_pydantic=True,
)

client = InfrahubClientSync(config=Config(address="http://infrahub-server-1:8000"))

query = """query AllDevicesArtifactsQuery {
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


@activity.defn(name="get_artifacts")
async def get_artifacts(occured_at: str) -> dict[str, dict[str, Any]]:
    data = client.execute_graphql(query=query)
    artifact_ids: dict[str, str] = {}
    artifacts: dict[str, str] = {}
    occured_at_format_code = "%Y-%m-%d %H:%M:%S.%f%z"
    updated_at_format_code = "%Y-%m-%dT%H:%M:%S.%f%z"

    for device in data["InfraDevice"]["edges"]:
        for artifact in device["node"]["artifacts"]["edges"]:
            updated_at = artifact["node"]["storage_id"]["updated_at"]
            updated_at_dt = datetime.strptime(updated_at, updated_at_format_code)
            occured_at_dt = datetime.strptime(occured_at, occured_at_format_code)

            if updated_at_dt >= occured_at_dt:
                artifact_ids[device["node"]["name"]["value"]] = artifact["node"]["storage_id"]["value"]

    for device, artifact_id in artifact_ids.items():
        response = client.object_store.get(identifier=artifact_id)
        data = yaml.load(response, Loader=yaml.SafeLoader)
        artifacts[device] = data

    return artifacts


@workflow.defn(sandboxed=False, name="DeploymentWorkflow")
class Deployment:
    @workflow.run
    async def run(self, occured_at: str) -> dict[str, dict[str, Any]]:
        from temporalio.workflow import asyncio

        artifacts = await workflow.execute_activity(
            activity=get_artifacts,
            arg=occured_at,
            schedule_to_close_timeout=timedelta(seconds=10),
        )

        child_promises = []
        for device, artifact in artifacts.items():
            child_promises.append(
                workflow.execute_child_workflow(
                    workflow=ConfigureDevice.run,
                    args=[device, artifact],
                    id=f"{device}-configure-{workflow.info().workflow_id}",
                    task_queue="default_queue",
                )
            )
        results = await asyncio.gather(*child_promises)
        workflow.logger.info("Child workflows finished. Results: %s", results)
        return results


@workflow.defn(sandboxed=False, name="ConfigureDeviceWorkflow")
class ConfigureDevice:
    @workflow.run
    async def run(self, device: str, artifact: dict[str, Any]) -> dict[str, Any]:
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
    activities=[get_artifacts, configure_device],
)


async def run_deployment_workflow(occured_at: str) -> WorkflowHandle:
    client = await Client.connect("temporal:7233", namespace="default")

    # Use a unique ID for each workflow execution to allow concurrent runs.
    workflow_id = f"deployment-{uuid.uuid4()}"
    return await client.start_workflow(
        workflow=Deployment.run,
        arg=occured_at,
        start_delay=timedelta(seconds=10),
        id=workflow_id,
        task_queue="default_queue",
    )


fastapi_app = FastAPI(docs_url="/doc")
app.add_asgi_worker("asgi_worker", fastapi_app, "0.0.0.0", 8001)


@fastapi_app.post("/deployment_workflow")
async def deployment_workflow(payload: dict[str, Any]) -> str:
    handle = await run_deployment_workflow(payload["occured_at"])
    return f"Workflow {handle.id} started."


if __name__ == "__main__":
    app.run()
