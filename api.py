import logging
from typing import Any

from fastapi import FastAPI
from temporal_boost import BoostApp
from temporalio.client import Client, WorkflowHandle

from worker import PortkeyWorkflow, ProposedChangeWorkflow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
TEMPORAL_SERVER = "temporal:7233"


app = BoostApp(
    name="Api",
    temporal_endpoint=TEMPORAL_SERVER,
    temporal_namespace="default",
    use_pydantic=True,
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
