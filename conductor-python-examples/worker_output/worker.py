

from conductor.client.http.models import Task, TaskResult
from conductor.client.http.models.task_result_status import TaskResultStatus
from conductor.client.worker.worker_interface import WorkerInterface
from conductor.client.configuration.configuration import Configuration
from conductor.client.configuration.settings.authentication_settings import AuthenticationSettings
from conductor.client.automator.task_handler import TaskHandler
from conductor.client.worker.worker import Worker
from conductor.client.worker.worker_task import WorkerTask

### Add these lines if not running on unix###
#from multiprocessing import set_start_method
#set_start_method("spawn", force=True)
#############################################
SERVER_API_URL = ""
KEY_ID = ""
KEY_SECRET = ""

"""
# Create worker using a class implemented by WorkerInterface
class SimplePythonWorker(WorkerInterface):
    def execute(self, task: Task) -> TaskResult:
        task_result = self.get_task_result_from_task(task)
        task_result.add_output_data('worker_style', 'class')
        task_result.add_output_data('secret_number', 1234)
        task_result.add_output_data('is_it_true', False)
        task_result.status = TaskResultStatus.COMPLETED
        return task_result

    def get_polling_interval_in_seconds(self) -> float:
        # poll every 500ms
        return 4

# Create worker using a WorkerTask decorator
@WorkerTask(task_definition_name='python_annotated_task', worker_id='decorated')
def python_annotated_task(input) -> object:
    return {'message': 'python is so great :)'}

# Create worker with domain using a WorkerTask decorator 
@WorkerTask(task_definition_name='python_annotated_task', worker_id='decorated_domain', domain='cool')
def python_annotated_task_with_domain(input) -> object:
    return {'message': 'python is so great and cool :)'}
"""

import datetime
import subprocess

def git_commit_csv(file_path, commit_message, repo_path):
    print(file_path, commit_message, repo_path)
    try:
        # Add the CSV file to the staging area
        subprocess.run(["git", "add", file_path], cwd=repo_path, check=True)

        # Commit the CSV file
        subprocess.run(["git", "commit", "-m", commit_message], cwd=repo_path, check=True)

        # Push the changes to the remote repository
        subprocess.run(["git", "push"], cwd=repo_path, check=True)

        print("CSV file committed and pushed to GitHub repository successfully.")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while committing the CSV file: {e}")


# Create worker using a function
def execute(task: Task) -> TaskResult:
    print(task.input_data)
    task_result = TaskResult(
        task_id=task.task_id,
        workflow_instance_id=task.workflow_instance_id,
        worker_id='test_111111'
    )
    csv_url = task.input_data.get("csv_url")
    print(csv_url)
    df = pd.read_csv(csv_url)
    file_path = "POCS/conductor-python-examples/worker_output_"+str(datetime.datetime.now())
    try:
        commit_message = "Added worker output"
        repo_path = "POCS/"
        df.to_json(file_path)
        git_commit_csv(file_path, commit_message, repo_path)
        task_result.add_output_data('worker_style', 'function')
        task_result.add_output_data('output', df.to_json())
        task_result.status = TaskResultStatus.COMPLETED
    
    except Exception as e:
        print(e)
    task_result.status = TaskResultStatus.FAILED
    return task_result

configuration = Configuration(
    server_api_url=SERVER_API_URL,
    debug=True,
    authentication_settings=AuthenticationSettings(
        key_id=KEY_ID,
        key_secret=KEY_SECRET
    ),
)

workers = [
    Worker(
        task_definition_name='worker_task_csv_to_json',
        execute_function=execute,
        poll_interval=5,
    )
]

# If there are decorated workers in your application, scan_for_annotated_workers should be set
# default value of scan_for_annotated_workers is False
with TaskHandler(workers, configuration, scan_for_annotated_workers=False) as task_handler:
    task_handler.start_processes()
    task_handler.join_processes()

