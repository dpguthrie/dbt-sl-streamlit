# stdlib
import os
import time
import uuid

# third party
import requests
from langchain_core.tools import tool

POST_URL = os.environ["DOCS_POST_URL"]
GET_URL = os.environ["DOCS_GET_URL"]


def create_request(message: str) -> dict[str, str]:
    payload = {
        "definition": None,
        "inputs": {},
        "message": message,
        "session_id": str(uuid.uuid4()),
        "stream_channel_id": str(uuid.uuid4()),
    }
    response = requests.post(POST_URL, json=payload)
    if not response.ok:
        return {
            "is_success": False,
            "message": f"Error creating request to the service. Error: {response.text}",
        }

    data = response.json()
    return {"is_success": True, "message": data["airops_app_execution"]["uuid"]}


def poll_for_request(uuid: str):
    url = f"{GET_URL}/{uuid}"
    while True:
        response = requests.get(url)
        if response.ok:
            data = response.json()
            status = data["status"]
            if status == "success":
                return data["output"]["response"]

            time.sleep(1)

        else:
            return None


@tool
def get_docs(message: str):
    """
    From a user's message, return an answer curated from dbt's documentation
    site: docs.getbt.com
    """
    request_dict = create_request(message)
    is_success = request_dict["is_success"]
    if not is_success:
        return request_dict["message"]

    uuid = request_dict["message"]
    return poll_for_request(uuid)
