import json
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

class JobQueue:
    """
    A file-system-based job queue where each job is a file named by its video_id,
    and the file content is the job data in JSON format.
    The oldest job (based on file creation time) is considered the first in the queue.
    """
    def __init__(self, path: str):
        """
        Initializes the job queue directory. Creates the directory if it doesn't exist.

        :param path: The path to the directory that will serve as the queue.
        """
        self.queue_path = Path(path)
        self.queue_path.mkdir(parents=True, exist_ok=True)

    def _get_file_path(self, video_id: str) -> Path:
        """Returns the full path for a given video ID."""
        return self.queue_path / f'{video_id}.json'

    def push(self, video_id: str, job_data: Dict[str, Any]) -> None:
        """
        Pushes a new job to the queue.

        :param video_id: The unique ID for the job (used as filename).
        :param job_data: The data associated with the job (must be JSON serializable).
        """
        file_path = self._get_file_path(video_id)
        if file_path.exists():
            # Overwrite existing job for simplicity, or raise an error/log a warning
            pass

        content = json.dumps(job_data, indent=4)
        file_path.write_text(content, encoding='utf-8')

    def pop(self) -> Optional[Tuple[str, Dict[str, Any]]]:
        """
        Removes the job that first entered the queue (oldest file) and returns it.
        Uses file creation time to determine the oldest entry.

        :return: A tuple of (video_id, job_data) or None if the queue is empty.
        """
        try:
            # Find the file with the minimum creation time (st_ctime)
            # Note: st_ctime is the file creation time on some systems (like Windows)
            # but is the last metadata change time on others (like Unix/Linux).
            # For a queue managed by a single process, this is usually acceptable.
            oldest_file = min(
                (p for p in self.queue_path.iterdir() if p.is_file()),
                key=lambda p: p.stat().st_ctime
            )
        except ValueError:
            # Raised if the sequence is empty (queue is empty)
            return None

        video_id = oldest_file.stem
        try:
            # Read and remove the file
            job_data = self._read_and_delete_file(oldest_file)
            return video_id, job_data
        except Exception as e:
            # Log error or handle corrupted/unreadable file
            print(f"Error processing oldest job file {oldest_file.name}: {e}")
            return None

    def check_existence(self, video_id: str) -> bool:
        """
        Checks if a job with the given video_id exists inside the queue.

        :param video_id: The ID of the video to check.
        :return: True if the job exists, False otherwise.
        """
        file_path = self._get_file_path(video_id)
        return file_path.is_file()

    def remove_from_queue(self, video_id: str) -> Optional[Dict[str, Any]]:
        """
        Removes the job by its video_id and returns the job data.

        :param video_id: The ID of the job to remove.
        :return: The job data (Dict) or None if the job was not found.
        """
        file_path = self._get_file_path(video_id)
        if not file_path.is_file():
            return None

        try:
            job_data = self._read_and_delete_file(file_path)
            return job_data
        except Exception as e:
            print(f"Error removing job file {video_id}: {e}")
            return None

    def _read_and_delete_file(self, file_path: Path) -> Dict[str, Any]:
        """Helper to read the content of a file, delete it, and return parsed JSON."""
        # Read content
        content = file_path.read_text(encoding='utf-8')
        job_data = json.loads(content)

        # Delete the file
        file_path.unlink()

        return job_data

    def __len__(self) -> int:
        """Returns the number of jobs in the queue."""
        return len([path for path in self.queue_path.glob('*.json')])
        # return sum(1 for p in self.queue_path.iterdir() if p.is_file())

    def __repr__(self) -> str:
        """String representation of the JobQueue."""
        return f"JobQueue(path='{self.queue_path}', size={len(self)})"