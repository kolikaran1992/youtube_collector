import time
import random
import sys
import traceback
import re
from pathlib import Path
from uuid import uuid1
from typing import Dict, Any, Optional
from slack_messenger import send_slack_message

# --- External Tool Imports ---
try:
    from omniconf import config as YT_CONFIG, logger
except ImportError:
    print("Error: omniconf is not available. Ensure it's set up to run this script.")
    sys.exit(1)

try:
    from yt_dlp import YoutubeDL
except ImportError:
    logger.error(
        "yt-dlp is not installed. Please install it using 'poetry add yt-dlp'."
    )
    sys.exit(1)

# Import the new, local JobQueue class
from yt_collector.job_queue import JobQueue

# --- Configuration Loading and Constants ---
SLACK_MESSAGE_HEADER = f"*YT-COLLECTOR-{uuid1().hex[:8]}: `{Path(__file__).name}`*"

try:
    # Channels for the standard (caption-fetching) flow
    CHANNELS_TO_MONITOR: list[str] = YT_CONFIG.channels_to_monitor
    # Channels for the video-download flow
    CHANNELS_TO_MONITOR_PLUS_DOWNLOAD: list[str] = (
        YT_CONFIG.channels_to_monitor_plus_video_download
    )

    MAX_NEW_URLS: int = YT_CONFIG.MAX_NEW_URLS_TO_FETCH

    # Initialize Job Queues
    # Standard queue (default destination)
    CAPTION_FETCHING_QUEUE: JobQueue = JobQueue(YT_CONFIG.yt_caption_fetching_queue_dir)
    # New queue for videos that require full download
    VIDEO_DOWNLOAD_QUEUE: JobQueue = JobQueue(YT_CONFIG.yt_video_download_queue_dir)

    # Downstream queues (used for existence checks)
    YT_INFO_QUEUE: JobQueue = JobQueue(YT_CONFIG.yt_info_fetching_queue_dir)
    ARJAN_CODES_QUEUE: JobQueue = JobQueue(YT_CONFIG.arjan_codes_queue)
    RESTING_QUEUE: JobQueue = JobQueue(YT_CONFIG.resting_queue_dir)
except AttributeError as e:
    print(f"Error: Required configuration value not found in config: {e}")
    sys.exit(1)
except Exception as e:
    print(f"Error loading configuration: {e}")
    sys.exit(1)


# --- Utility Functions (Pulled in to centralize logic and reduce dependency) ---


def sanitize_filename(text: str) -> str:
    """Converts text to a clean, lowercase, underscore-separated string suitable for directory names."""
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[-\s]+", "_", text).strip("_")
    return text


def video_id_already_exists_in_system(video_id: str) -> bool:
    """
    Checks if a video is already in any downstream processing/output queue
    to prevent reprocessing/duplicate entries.
    """

    caption_output_directory = Path(YT_CONFIG.kaggle_output_dir_yt_captions).resolve()
    info_output_dir = Path(YT_CONFIG.kaggle_output_dir_yt_info).resolve()

    for base_dir in [caption_output_directory, info_output_dir]:
        if not base_dir.exists():
            continue
        # Check files recursively for the video_id in the filename
        for path in base_dir.rglob("*"):
            if path.is_file() and video_id in path.name:
                return True

    if (
        RESTING_QUEUE.check_existence(video_id)
        or YT_INFO_QUEUE.check_existence(video_id)
        or CAPTION_FETCHING_QUEUE.check_existence(video_id)
        or VIDEO_DOWNLOAD_QUEUE.check_existence(video_id)
        or ARJAN_CODES_QUEUE.check_existence(video_id)
    ):
        return True

    return False


# --- Core Fetching Logic (Now self-contained in cron script) ---


def fetch_top_urls_and_push(
    raw_channel_name: str, target_queue: JobQueue, channel_type: str
) -> int:
    """
    Fetches minimal metadata for new videos/shorts from a channel and pushes them to the JobQueue.

    Returns:
        int: The total number of new video URLs added to the queue.
    """

    new_urls_processed = 0
    # Dynamically construct the URL to fetch /videos or /shorts
    channel_url = f"https://www.youtube.com/@{raw_channel_name}/{channel_type}"

    logger.info("--- YouTube Minimal Metadata Fetcher (Integrated) ---")
    logger.info(f"Channel URL: {channel_url}")
    logger.info(f"Max New URLs to Process: {MAX_NEW_URLS}")

    # YoutubeDL options for fast, flat extraction
    channel_ydl_opts: Dict[str, Any] = {
        "extract_flat": "in_playlist",
        "quiet": True,
        "skip_download": True,
        "noplaylist": True,
        "cookiesfrombrowser": ("chrome", "Default"),
    }

    with YoutubeDL(channel_ydl_opts) as ydl:
        info_dict = ydl.extract_info(channel_url, download=False)

        if info_dict is None or "entries" not in info_dict:
            logger.warning(
                "Could not find video entries from the channel URL. Aborting."
            )
            return 0

        entries = info_dict.get("entries", [])
        logger.info(f"Found {len(entries)} entries in the fast list.")

        # Iterate through entries (which are sorted Newest -> Oldest)
        for entry in entries:
            if new_urls_processed >= MAX_NEW_URLS:
                logger.info(
                    f"Limit of {MAX_NEW_URLS} new URLs reached. Stopping iteration."
                )
                break

            video_id: Optional[str] = entry.get("id")
            video_url: Optional[str] = entry.get("url")

            if not video_id or not video_url:
                continue

            if video_id_already_exists_in_system(video_id):
                continue

            # 1. Extract minimal metadata
            minimal_metadata: Dict[str, Any] = {
                "url": video_url,
                "video_id": video_id,
                "title": entry.get("title", "No Title"),
                "description": entry.get("description"),
                "view_count": entry.get("view_count"),
                "channel_name": raw_channel_name,  # Add raw channel name for context
            }

            logger.info(
                f"  [PROCESS] New video {video_id}. Title: {minimal_metadata['title']}"
            )

            # 2. Push to JobQueue
            target_queue.push(video_id, minimal_metadata)
            new_urls_processed += 1

        logger.info("--- Fetch Complete ---")
        logger.info(f"Total new videos added to queue: {new_urls_processed}")
        return new_urls_processed


def calculate_total_queue_size() -> int:
    """
    Calculates the total size of all job files under the primary queues.
    """
    return len(CAPTION_FETCHING_QUEUE) + len(VIDEO_DOWNLOAD_QUEUE)


def run_cron_fetcher():
    # Define the list of channels, their corresponding queues, and the YouTube section to fetch
    CHANNELS_WITH_QUEUES_AND_TYPE = []
    # 1. Standard Channels -> Caption Queue (fetches /videos)
    for channel in CHANNELS_TO_MONITOR:
        CHANNELS_WITH_QUEUES_AND_TYPE.append(
            (channel, CAPTION_FETCHING_QUEUE, "videos")
        )
    # 2. Video Download Channels -> Video Download Queue (fetches /shorts)
    for channel in CHANNELS_TO_MONITOR_PLUS_DOWNLOAD:
        CHANNELS_WITH_QUEUES_AND_TYPE.append((channel, VIDEO_DOWNLOAD_QUEUE, "shorts"))

    total_channels = len(CHANNELS_WITH_QUEUES_AND_TYPE)
    logger.info(f"Starting cron fetcher for {total_channels} channels...")
    total_new_files_added = 0  # Initialize counter

    for i, (channel_name, target_queue, channel_type) in enumerate(
        CHANNELS_WITH_QUEUES_AND_TYPE
    ):
        queue_name = target_queue.queue_path.name
        logger.info(
            f"\
--- Processing Channel {i+1}/{total_channels} (Queue: {queue_name}, Type: {channel_type}) ---"
        )
        logger.info(f"Channel URL: {channel_name}")

        # 1. Fetch URLs and push to the channel-specific JobQueue
        try:
            new_files_count = fetch_top_urls_and_push(
                channel_name, target_queue, channel_type
            )  # Updated call
            total_new_files_added += new_files_count
        except Exception as e:
            tb = traceback.format_exc()
            error_message = (
                f"🚨 *Channel Error*: Failed to process channel `{channel_name}` (Queue: {queue_name}, Type: {channel_type}).\n"
                f"*Error Details*: {e}\n"
                f"*Traceback*:\n```{tb}\n```"
            )
            logger.error(
                f"An error occurred while processing channel {channel_name}: {e}"
            )
            send_slack_message(message=error_message, header=SLACK_MESSAGE_HEADER)

        # 2. Add Jitter (Delay) between 0 and 60 seconds for the next channel
        if i < total_channels - 1:
            jitter_seconds = random.randint(0, 60)
            logger.info(
                f"Waiting for a random jitter of {jitter_seconds} seconds before the next channel..."
            )
            time.sleep(jitter_seconds)

    logger.info(
        "\
Cron fetcher completed."
    )

    # Calculate final metrics for the Slack message
    total_queue_size = calculate_total_queue_size()

    send_slack_message(
        message=(
            f"✅ `Cron Job Success`: URL fetching completed for {total_channels} channels.\
"
            f"*Metrics Summary*\
"
            f"`New URLs added to queue`: *{total_new_files_added}*\
"
            f"`Current total queue size`: *{total_queue_size}*"
        ),
        header=SLACK_MESSAGE_HEADER,
    )


if __name__ == "__main__":
    # Calculate total channels for the initial message
    total_channels = list(
        set(list(CHANNELS_TO_MONITOR) + list(CHANNELS_TO_MONITOR_PLUS_DOWNLOAD))
    )
    send_slack_message(
        message=f"🚀 `Cron Job Start`: Fetching URLs for {total_channels} channels",
        header=SLACK_MESSAGE_HEADER,
    )
    run_cron_fetcher()
