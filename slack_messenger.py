import os, requests
from omniconf import config, logger


def format_message_in_box(message: str, header: str) -> str:
    message_lines = message.split('\n')
    # max_len = max(len(line) for line in message_lines)
    max_len = len(header)
    
    # add 2 spaces padding on each side
    horizontal_border = '-' * (max_len + 4)
    boxed_lines = []
    for line in message_lines:
        boxed_lines.append(f"{line.ljust(max_len)}")
    
    message_box = '\n'.join(boxed_lines)
    formatted_message = f"{horizontal_border}| {header} |{horizontal_border}\n{message_box}"
    return formatted_message


def format_message_in_box(message: str, header: str) -> str:
    MAX_WIDTH = 100  # 🔹 cap total width here

    # Determine available space for header text
    padding = 6  # 3 '=' on each side of header, or adjust as you like
    inner_width = MAX_WIDTH - len(header) - padding
    if inner_width < 0:
        inner_width = 0  # fallback if header is too long

    # Split '=' evenly on both sides
    left_eq = inner_width // 2
    right_eq = inner_width - left_eq

    horizontal_border = (
        f"{'=' * left_eq}| {header} |{'=' * right_eq}"
    )

    message_lines = message.split('\n')
    message_box = '\n'.join(message_lines)

    formatted_message = f"{horizontal_border}\n{message_box}"
    return formatted_message

def send_slack_message(
    message: str,
    header: str,
    slack_bot_token: str = None,
    slack_channel_id: str = None
) -> None:
    """
    Sends a structured notification message to the Slack channel configured in config.

    Args:
        title (str): Title of the message.
        status (str): Status string, e.g., "CRITICAL ALERT" or "CRON RUN COMPLETE".
        details (str): Detailed description or log of the event.
        job_id (str): Optional job identifier.
    """
    slack_bot_token = config.slack.bot_token
    slack_channel_id = config.slack.channel_id
    
    if not slack_bot_token or not slack_channel_id:
        logger.info(
            f"empty SLACK_BOT_TOKEN: '{slack_bot_token}' or SLACK_CHANNEL_ID: '{slack_channel_id}', not sending slack notification"
        )
        return
    
    headers = {
        "Authorization": f"Bearer {slack_bot_token}",
        "Content-Type": "application/json",
    }
    payload = {"channel": slack_channel_id, "text": format_message_in_box(message, header)}

    try:
        response = requests.post(
            "https://slack.com/api/chat.postMessage", json=payload, headers=headers
        )
        data = response.json()
        if not data.get("ok"):
            raise Exception(f"Slack API error: {data}")
        logger.info("Slack message sent successfully.")
    except Exception as e:
        logger.error(f"Failed to send Slack message: {e}")
