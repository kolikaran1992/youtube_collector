import json
import re
from pathlib import Path
from typing import Dict, Any

from omniconf import config

# Access configuration variables from the [yt_collector] section once at module level
YT_CONFIG = config

def sanitize_filename(text: str) -> str:
    """Converts text to a clean, lowercase, underscore-separated string suitable for directory names or file names."""
    # Convert to lowercase
    text = text.lower()
    # Remove characters that are not letters, numbers, or spaces/underscores
    text = re.sub(r'[^\w\s-]', '', text)
    # Replace spaces and hyphens with underscores
    text = re.sub(r'[-\s]+', '_', text).strip('_')
    return text


def get_all_queue_paths_absolute() -> list[str]:
    """
    Returns a list of all absolute file paths of JSON files 
    across all channel subdirectories in the main queue directory.
    """
    base_dir = Path(YT_CONFIG.remaining_video_url_queue)
    # Recursive glob to find all *.json files in all subdirectories
    return [str(p.resolve()) for p in base_dir.glob("**/*.json")]

def check_if_video_in_processing_queue(video_id: str) -> bool:
    caption_input_directory_queue_level_two = Path(config.PROCESSING_CAPTION_VIDEO_URL_QUEUE).resolve()
    caption_output_directory = Path(config.VIDEO_AUTOMATIC_CAPTIONS).resolve()
    post_caption_queue_level_two = Path(config.processing_info_collection_video_url_queue).resolve()
    
    for base_dir in [caption_input_directory_queue_level_two, caption_output_directory, post_caption_queue_level_two]:
        if not base_dir.exists():
            continue  # skip if path doesn’t exist
        for path in base_dir.rglob('*'):
            if path.is_file() and video_id in path.name:
                return True
    return False    
    

class ChannelLocalStorage:
    """
    Manages local storage paths and queue operations for a specific YouTube channel.
    """
    
    def __init__(self, raw_channel_name: str):
        """
        Initializes the storage manager for a channel, sanitizing the name 
        to create the path component.
        """
        self.raw_channel_name = raw_channel_name
        self.channel_path_component = sanitize_filename(raw_channel_name)

    def _get_channel_dir_path(self, config_key: str) -> Path:
        """
        Internal method to construct the channel-specific Path object 
        for a given config key and ensure the directory exists.
        """
        base_dir = Path(getattr(YT_CONFIG, config_key))
        channel_dir = base_dir / self.channel_path_component
        channel_dir.mkdir(parents=True, exist_ok=True)
        return channel_dir

    @property
    def queue_dir(self) -> Path:
        """Returns the Path object for the channel's video URL queue directory."""
        return self._get_channel_dir_path('remaining_video_url_queue')

    @property
    def metadata_dir(self) -> Path:
        """Returns the Path object for the channel's downloaded video metadata directory."""
        return self._get_channel_dir_path('downloaded_video_metadata_dir')

    @property
    def content_dir(self) -> Path:
        """Returns the Path object for the channel's video content directory."""
        return self._get_channel_dir_path('video_content_dir')

    # --- Queue Utility Methods ---

    def is_video_in_queue(self, video_id: str) -> bool:
        """Checks if a video's JSON metadata file ({id}.json) already exists in the channel's queue directory."""
        file_path = self.queue_dir / f"{video_id}.json"
        return file_path.exists()

    def save_metadata_to_queue(self, video_id: str, data: Dict[str, Any]) -> Path:
        """Saves the minimal video metadata to a JSON file in the channel's queue directory."""
        file_path = self.queue_dir / f"{video_id}.json"
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            
        return file_path