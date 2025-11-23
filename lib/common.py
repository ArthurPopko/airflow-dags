import logging

import requests
from airflow.hooks.base import BaseHook


def send_slack_message(conn_id, channel, message: str):
    conn = BaseHook.get_connection(conn_id)
    token = conn.password

    logging.info("Sending message to Slack...")

    resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {token}"},
        data={
            "channel": channel,
            "text": message,
        },
    )

    if not resp.ok or not resp.json().get("ok"):
        logging.error(f"Failed to send message to Slack: {resp.text}")


if __name__ == "__main__":
    pass
