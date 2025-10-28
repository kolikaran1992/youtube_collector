import sys
import argparse
from pathlib import Path
from typing import Optional, Dict, Any

try:
    from omniconf import config
    # Assume logger is directly importable from omniconf as requested
    from omniconf import logger
except ImportError:
    # Use print for fatal errors before logger setup is confirmed
    print("FATAL ERROR: omniconf is not available. Ensure it's set up to run this script.")
    sys.exit(1)

try:
    from yt_dlp import YoutubeDL
except ImportError:
    logger.error("yt-dlp is not installed. Please install it using 'poetry add yt-dlp'.")
    sys.exit(1)

# Import the new utility class
from yt_collector.local_storage_utils import ChannelLocalStorage, check_if_video_in_processing_queue

# --- Configuration Loading ---
YT_CONFIG = config
# Use the new configuration key
MAX_NEW_URLS: int = YT_CONFIG.MAX_NEW_URLS


# --- Main Fetching Logic (Minimal Metadata Pipeline) ---

def fetch_top_urls_and_save(raw_channel_name: str) -> int:
    """
    Implements the fast fetch pipeline:
    1. Constructs the channel URL from the raw channel name.
    2. Instantiates ChannelLocalStorage for the channel.
    3. Fast URL Fetch (Newest -> Oldest) using extract_flat: 'in_playlist'.
    4. Limits processing to MAX_NEW_URLS.
    5. Extracts minimal metadata and saves it to the queue directory using the ChannelLocalStorage instance.
    
    Returns:
        int: The total number of new video URLs added to the queue.
    """
    
    # Initialize the channel-specific storage manager
    storage = ChannelLocalStorage(raw_channel_name)
    new_urls_processed = 0
    
    # 1. Construct the URL as requested
    channel_url = f"https://www.youtube.com/@{raw_channel_name}/videos"
    
    logger.info("--- YouTube Minimal Metadata Fetcher ---")
    logger.info(f"Channel URL: {channel_url}")
    logger.info(f"Max New URLs to Process: {MAX_NEW_URLS}")

    # 2. Fast URL Fetch: Get a minimal list of URLs and IDs
    channel_ydl_opts: Dict[str, Any] = {
        "extract_flat": "in_playlist",
        "quiet": True,
        "skip_download": True,
        "noplaylist": True,
        "cookiesfrombrowser": ("chrome", "Default"),
    }

    try:
        with YoutubeDL(channel_ydl_opts) as ydl:
            info_dict = ydl.extract_info(channel_url, download=False)

            if info_dict is None or "entries" not in info_dict:
                logger.warning("Could not find video entries from the channel URL. Aborting.")
                return 0 # Return 0 new items on abort/failure

            entries = info_dict.get("entries", [])
            logger.info(f"Found {len(entries)} entries in the fast list.")

            # Use the Path property from the storage object for logging/tracking.
            channel_queue_dir: Path = storage.queue_dir 

            logger.info(f"Target Queue Directory: {channel_queue_dir}")

            # new_urls_processed = 0 (Removed, initialized outside 'try')

            # Iterate through entries (which are sorted Newest -&gt; Oldest)
            for i, entry in enumerate(entries):
                if new_urls_processed >= MAX_NEW_URLS:
                    logger.info(f"Limit of {MAX_NEW_URLS} new URLs reached. Stopping iteration.")
                    break

                # Basic check for essential data
                video_id: Optional[str] = entry.get("id")
                video_url: Optional[str] = entry.get("url")

                if not video_id or not video_url:
                    continue

                # 4. Skip Existing Check - Use the storage object method
                if storage.is_video_in_queue(video_id) or check_if_video_in_processing_queue(video_id):
                    # Consider using logger.debug here if log volume is high
                    continue

                # 5. Extract ONLY the required minimal metadata
                minimal_metadata: Dict[str, Any] = {
                    "url": video_url,
                    "video_id": video_id,
                    "title": entry.get("title", "No Title"),
                    "description": entry.get("description"),
                    "view_count": entry.get("view_count"),
                }

                logger.info(f"  [PROCESS] New video {video_id}. Title: {minimal_metadata['title']}")

                # 6. Save to Queue - Use the storage object method
                storage.save_metadata_to_queue(video_id, minimal_metadata)
                new_urls_processed += 1

            logger.info("\n--- Fetch Complete ---")
            logger.info(f"Total new videos added to queue: {new_urls_processed}")
            logger.info(f"Queue files saved to: {channel_queue_dir}")
            return new_urls_processed


    except Exception as e:
        logger.error(f"An unexpected error occurred during channel fetch: {e}", exc_info=True)
        return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch top new video URLs for a given YouTube channel name.")
    parser.add_argument("channel_name", nargs="?", default="SimonSquibb", help="The raw YouTube channel name (e.g., SimonSquibb).")
    args = parser.parse_args()
    
    # Pass the raw channel name directly
    fetch_top_urls_and_save(args.channel_name)