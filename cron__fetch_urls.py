import time
import random
import sys
import traceback
from pathlib import Path
from slack_messenger import send_slack_message

try:
    from omniconf import config
except ImportError:
    print("Error: omniconf is not available. Ensure it's set up to run this script.")
    sys.exit(1)

# Import the core fetching function
try:
    from yt_collector.fetch_urls import fetch_top_urls_and_save
    from yt_collector.local_storage_utils import get_all_queue_paths_absolute # Added utility
except ImportError as e:
    print(f"Error: Could not import necessary modules for URL fetching: {e}")
    sys.exit(1)

# --- Configuration Loading ---
# The list of channels to monitor is expected to be under the [yt_collector] section
try:
    YT_CONFIG = config
    SLACK_MESSAGE_HEADER = f'*YT-COLLECTOR-JOB: `{Path(__file__).name}`*'
    CHANNELS_TO_MONITOR = YT_CONFIG.channels_to_monitor
except AttributeError:
    print("Error: Configuration value 'channels_to_monitor' not found in [yt_collector] config.")
    sys.exit(1)
except Exception as e:
    print(f"Error loading configuration: {e}")
    sys.exit(1)


def run_cron_fetcher():
    print(f"Starting cron fetcher for {len(CHANNELS_TO_MONITOR)} channels...")
    total_new_files_added = 0 # Initialize counter

    for i, channel_name in enumerate(CHANNELS_TO_MONITOR):
        print(f"\n--- Processing Channel {i+1}/{len(CHANNELS_TO_MONITOR)} ---")
        print(f"Channel URL: {channel_name}")

        # 1. Fetch URLs
        try:
            # Capture the number of new files added
            new_files_count = fetch_top_urls_and_save(channel_name)
            total_new_files_added += new_files_count
        except Exception as e:
            tb = traceback.format_exc()
            error_message = (
                f"🚨 **Channel Error:** Failed to process channel `{channel_name}`.\n"
                f"**Error Details:** {e}\n"
                f"**Traceback:**\n```\n{tb}\n```"
            )
            print(f"An error occurred while processing channel {channel_name}: {e}")
            send_slack_message(message=error_message) # Send detailed traceback on failure

        # 2. Add Jitter (Delay) between 0 and 60 seconds for the next channel
        if i < len(CHANNELS_TO_MONITOR) - 1:
            # Only apply delay if it's not the last channel
            jitter_seconds = random.randint(0, 60)
            print(f"Waiting for a random jitter of {jitter_seconds} seconds before the next channel...")
            time.sleep(jitter_seconds)

    print("\nCron fetcher completed.")
    
    # Calculate final metrics for the Slack message
    total_queue_size = len(get_all_queue_paths_absolute())
    
    send_slack_message(
        message=(
            f"✅ `Cron Job Success`: URL fetching completed for {len(CHANNELS_TO_MONITOR)} channels.\n"
            f"*Metrics Summary*\n"
            f"`New URLs added to queue`: *{total_new_files_added}*\n"
            f"`Current total queue size`: *{total_queue_size}*"
        ),
        header = SLACK_MESSAGE_HEADER
    )


if __name__ == "__main__":
    send_slack_message(
        message=f"🚀 `Cron Job Start`: Fetching URLs for {len(CHANNELS_TO_MONITOR)} channels",
        header=SLACK_MESSAGE_HEADER
    )
    run_cron_fetcher()