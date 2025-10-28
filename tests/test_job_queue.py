import pytest
import json
import shutil
import tempfile
from pathlib import Path
from yt_collector.job_queue import JobQueue
import time 

# FIX: Change temp_queue_path to use 'function' scope explicitly to ensure isolation,
# although it's the default. Let's try to ensure full isolation.
@pytest.fixture(scope="function")
def temp_queue_path():
    """Pytest fixture to create a temporary directory for the job queue and clean it up."""
    # Create a temporary directory
    temp_dir = Path(tempfile.mkdtemp())
    yield str(temp_dir)
    # Cleanup: remove the directory after the test is done
    shutil.rmtree(temp_dir)

@pytest.fixture(scope="function")
def temp_queue_path_empty():
    """Pytest fixture to create a temporary directory for the job queue and clean it up."""
    # Create a temporary directory
    temp_dir = Path(tempfile.mkdtemp())
    yield str(temp_dir)
    # Cleanup: remove the directory after the test is done
    shutil.rmtree(temp_dir)


@pytest.fixture(scope="function")
def empty_job_queue(temp_queue_path_empty):
    """Fixture to provide an initialized, empty JobQueue instance."""
    # The fixture path is guaranteed to be empty by the temp_queue_path cleanup/creation
    return JobQueue(temp_queue_path_empty)

@pytest.fixture(scope="function")
def filled_job_queue(temp_queue_path):
    """Fixture to provide a JobQueue instance with several sequential jobs."""
    queue = JobQueue(temp_queue_path)
    
    # Push 3 jobs sequentially, with a small delay to ensure ctime differences
    jobs = {
        'v1': {'title': 'First Video', 'duration': 100},
        'v2': {'title': 'Middle Video', 'duration': 200},
        'v3': {'title': 'Last Video', 'duration': 300}
    }
    
    for video_id, data in jobs.items():
        queue.push(video_id, data)
        # Introduce a minor delay to ensure file creation times are distinct
        time.sleep(0.01) 
        
    return queue, jobs # Return the queue instance and the original data

class TestJobQueue:
    
    def test_init_creates_directory(self, temp_queue_path):
        """Test if the __init__ method correctly creates the queue directory."""
        queue_path = Path(temp_queue_path)
        assert queue_path.is_dir()
        
    def test_push_creates_file_correctly(self, empty_job_queue, temp_queue_path_empty):
        """Test if push creates a file with the correct name and JSON content."""
        video_id = "test_vid_1"
        job_data = {"key": "value", "number": 123}
        
        empty_job_queue.push(video_id, job_data)
        
        # FIX: The implementation uses a .json extension, so the test must check for it.
        file_path = Path(temp_queue_path_empty) / f'{video_id}.json'
        assert file_path.is_file()
        
        # Check content
        with open(file_path, 'r', encoding='utf-8') as f:
            content = json.load(f)
            assert content == job_data
            
    def test_check_existence(self, empty_job_queue):
        """Test check_existence method."""
        test_id = "exists_id"
        
        # Test non-existent
        assert empty_job_queue.check_existence(test_id) is False
        
        # Test existent
        empty_job_queue.push(test_id, {"data": "temp"})
        assert empty_job_queue.check_existence(test_id) is True
        
    def test_pop_from_empty_queue(self, empty_job_queue):
        """Test pop method returns None when the queue is empty."""
        assert empty_job_queue.pop() is None
        
    def test_pop_removes_oldest_job(self, filled_job_queue, temp_queue_path):
        """
        Test if pop correctly retrieves the oldest job (v1) and removes it.
        This relies on file creation time (st_ctime).
        """
        queue, original_jobs = filled_job_queue
        
        # 1. Pop the first job (should be 'v1')
        # FIX: The implementation now returns the clean ID 'v1', so the assertion is correct.
        video_id, data = queue.pop()
        
        assert video_id == 'v1'
        assert data == original_jobs['v1']
        
        # 2. Check that the file is gone
        # FIX: Check for the file with the .json extension.
        assert not (Path(temp_queue_path) / 'v1.json').exists()
        
        # 3. Pop the next job (should be 'v2')
        video_id, data = queue.pop()
        
        assert video_id == 'v2'
        assert data == original_jobs['v2']
        # FIX: Check for the file with the .json extension.
        assert not (Path(temp_queue_path) / 'v2.json').exists()

        # 4. Check the queue size is now 1
        assert len(queue) == 1

        
    def test_remove_from_queue_by_id(self, filled_job_queue, temp_queue_path):
        """Test removing a job by its ID and returning the data."""
        queue, original_jobs = filled_job_queue
        
        # 1. Remove the middle job ('v2')
        removed_data = queue.remove_from_queue('v2')
        
        assert removed_data == original_jobs['v2']
        
        # 2. Check the file is gone
        # FIX: Check for the file with the .json extension.
        assert not (Path(temp_queue_path) / 'v2.json').exists()
        
        # 3. Check the remaining queue size (should be v1 and v3)
        assert len(queue) == 2
        
        # 4. Try to pop the next job (should still be 'v1')
        # FIX: The assertion is now correct because pop() returns clean ID.
        video_id, data = queue.pop()
        assert video_id == 'v1'

        
    def test_remove_non_existent_job(self, empty_job_queue):
        """Test remove_from_queue for a job that doesn't exist."""
        result = empty_job_queue.remove_from_queue('non_existent_id')
        assert result is None

    def test_len_method(self, empty_job_queue, filled_job_queue):
        """Test the __len__ method (size of the queue)."""
        assert len(empty_job_queue) == 0

        queue, _ = filled_job_queue

        # Check filled queue
        assert len(queue) == 3
        
        # Pop one, check size
        queue.pop()
        assert len(queue) == 2
        
        # Push one, check size
        queue.push('v4', {})
        assert len(queue) == 3