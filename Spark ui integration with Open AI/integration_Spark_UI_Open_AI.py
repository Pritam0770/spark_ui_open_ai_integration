from abc import ABC, abstractmethod
from dotenv import load_dotenv
from openai import OpenAI
import requests
import json
import os

# Get env values
load_dotenv()
organization = os.getenv("ORGANIZATION")
project = os.getenv("PROJECT")
api_key = os.getenv("API_KEY")

client = OpenAI(
  organization = organization,
  project = project,
  api_key = api_key
)

# Abstract Base Class for Spark Environment
class SparkEnvironment(ABC):
    def __init__(self):
        self.application_id = None
    
    @abstractmethod
    def get_application_id(self):
        pass
    
    @abstractmethod
    def get_all_job_details_of_application(self):
        pass
    
    @abstractmethod
    def get_specific_job_details(self):
        pass

    @abstractmethod
    def get_stage_details(self):
        pass
    
    @abstractmethod
    def get_task_details(self):
        pass
    
    def json_compaction(self, json_data):
        if isinstance(json_data, dict):
            return {k: self.json_compaction(v) for k, v in json_data.items() if v not in [0, 0.0, "", [], {}, None]}
        # If any list contains a single value which is not 0 then keep the whole list else return blank list
        elif isinstance(json_data, list):
            for item in json_data:
                if item not in [0.0, {}, [], None]:
                    return [self.json_compaction(item) for item in json_data]
            return []
        else: 
            return json_data

    def ask_openai(self, question, formatted_data):
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"user", "content":f"{question} \n {formatted_data}"}],
            stream=False,
        )
        return response.choices[0].message.content


# Class for Databricks Spark Environment
class DatabricksSparkEnvironment(SparkEnvironment):
    def __init__(self, databricks_token, databricks_instance, databricks_application_id = None):
        super().__init__()
        self.databricks_token = databricks_token
        self.databricks_instance = databricks_instance
        self.databricks_application_id = databricks_application_id

    def _databricks_api_get(self, endpoint):
        url = f"{self.databricks_instance}{endpoint}"
        headers = {"Authorization": f"Bearer {self.databricks_token}"}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch data from Databricks. Status: {response.status_code}")

    def get_application_id(self):
        jobs = self._databricks_api_get("/api/2.0/jobs/list")
        self.application_id = jobs['jobs'][0]['job_id'] if jobs else None
        return self.application_id

    def get_all_job_details_of_application(self):
        jobs_url = f"/api/v1/applications/{self.databricks_application_id}/jobs"
        response = requests.get(jobs_url)
        if response.status_code == 200:
            all_jobs = response.json()
            return all_jobs
        return None
    
    def get_specific_job_details(self, job_id):
        job_url = f"/api/v1/applications/{self.databricks_application_id}/jobs/{job_id}"
        response = requests.get(job_url)
        if response.status_code == 200:
            job_details = response.json()
            return job_details
        return None

    def get_stage_details(self, job_id):
        job_details = self.get_specific_job_details(job_id)
        if not job_details:
            print(f"Job ID {job_id} not found in application {self.databricks_application_id}.")
            return None
        
        # Extract stage IDs from the job details
        stage_ids = job_details.get('stageIds', [])
        
        stages = []
        for stage_id in stage_ids:
            stage_url = f"/api/v1/applications/{self.databricks_application_id}/stages/{stage_id}"
            stage_details = self._databricks_api_get(stage_url).json()
            stages.append(stage_details)
        return {
            "job_id" : job_id,
            "stages" : stages
        }

    def get_task_details(self, stage_id):
        task_url = f"/api/v1/applications/{self.databricks_application_id}/stages/{stage_id}"
        response = self._databricks_api_get(stage_url).json()
        if response.status_code == 200:
            task_details = response.json()
            return task_details
        return None

    def get_executor_details(self):
        cluster_id = "<your-cluster-id>"  # Fetch dynamically if needed
        executors = self._databricks_api_get(f"/api/2.0/clusters/get?cluster_id={cluster_id}")["executors"]
        return {"executors": executors}


# Class for non-Databricks Spark Environment
class NonDatabricksSparkEnvironment(SparkEnvironment):
    def __init__(self, spark_master_url, NonDatabricksSparkEnvironment_application_id = None):
        super().__init__()
        self.spark_master_url = spark_master_url
        self.NonDatabricksSparkEnvironment_application_id = NonDatabricksSparkEnvironment_application_id

    def get_application_id(self):
        url = f"{self.spark_master_url}/applications"
        response = requests.get(url)
        if response.status_code == 200:
            applications = response.json()
            self.application_id = applications[0]['id'] if applications else None
        return self.application_id
    
    def get_all_job_details_of_application(self):
        jobs_url = f"{self.local_spark_ui_url}/applications/{self.NonDatabricksSparkEnvironment_application_id}/jobs"
        response = requests.get(jobs_url)
        if response.status_code == 200:
            all_jobs = response.json()
            return all_jobs
        return None
    
    def get_specific_job_details(self, job_id):
        job_url = f"{self.spark_master_url}/applications/{self.NonDatabricksSparkEnvironment_application_id}/jobs/{job_id}"
        response = requests.get(job_url)
        if response.status_code == 200:
            job_details = response.json()
            return job_details
        return None

    def get_stage_details(self, job_id):
        job_details = self.get_specific_job_details(job_id)
        if not job_details:
            print(f"Job ID {job_id} not found in application {self.NonDatabricksSparkEnvironment_application_id}.")
            return None
        
        # Extract stage IDs from the job details
        stage_ids = job_details.get('stageIds', [])
        
        stages = []
        for stage_id in stage_ids:
            stage_url = f"{self.spark_master_url}/applications/{self.NonDatabricksSparkEnvironment_application_id}/stages/{stage_id}"
            stage_details = requests.get(stage_url).json()
            stages.append(stage_details)
        return {
            "job_id" : job_id,
            "stages" : stages
        }


    def get_task_details(self, stage_id):
        task_url = f"{self.spark_master_url}/applications/{self.NonDatabricksSparkEnvironment_application_id}/stages/{stage_id}/taskSummary"
        response = requests.get(stage_url).json()
        if response.status_code == 200:
            task_details = response.json()
            return task_details
        return None

    def get_executor_details(self):
        executors_url = f"{self.local_spark_ui_url}/applications/{self.local_application_id}/executors"
        executors = requests.get(executors_url).json()
        return {"executors": executors}


# Class for Local Spark Environment (Testing with localhost)
class LocalSparkEnvironment(SparkEnvironment):
    def __init__(self, local_spark_ui_url, local_application_id = None):
        super().__init__()
        self.local_spark_ui_url = local_spark_ui_url
        self.local_application_id = local_application_id

    #  Get lastest application's id
    def get_application_id(self):
        url = f"{self.local_spark_ui_url}/applications"
        response = requests.get(url)
        if response.status_code == 200:
            applications = response.json()
            self.application_id = applications[0]['id'] if applications else None
        return self.application_id

    def get_all_job_details_of_application(self):
        jobs_url = f"{self.local_spark_ui_url}/applications/{self.local_application_id}/jobs"
        response = requests.get(jobs_url)
        if response.status_code == 200:
            all_jobs = response.json()
            return all_jobs
        return None
    
    def get_specific_job_details(self, job_id):
        job_url = f"{self.local_spark_ui_url}/applications/{self.local_application_id}/jobs/{job_id}"
        response = requests.get(job_url)
        if response.status_code == 200:
            job_details = response.json()
            return job_details
        return None

    def get_stage_details(self, job_id):
        job_details = self.get_specific_job_details(job_id)
        if not job_details:
            print(f"Job ID {job_id} not found in application {self.local_application_id}.")
            return None
        
        # Extract stage IDs from the job details
        stage_ids = job_details.get('stageIds', [])
        
        stages = []
        for stage_id in stage_ids:
            stage_url = f"{self.local_spark_ui_url}/applications/{self.local_application_id}/stages/{stage_id}"
            stage_details = requests.get(stage_url)
            if stage_details.status_code == 200:
                stages.append(stage_details.json())
            else:
                stages.append(None)
        return {
            "job_id" : job_id,
            "stages" : stages
        }


    def get_task_details(self, stage_id, stage_attempt_id):
        task_url = f"{self.local_spark_ui_url}/applications/{self.local_application_id}/stages/{stage_id}/{stage_attempt_id}/taskSummary"
        response = requests.get(task_url)
        print(response.status_code)
        if response.status_code == 200:
            task_details = response.json()
            return task_details
        return None

    def get_executor_details(self):
        executors_url = f"{self.local_spark_ui_url}/applications/{self.local_application_id}/executors"
        
        executors = requests.get(executors_url).json()
        return {"executors": executors}



# Factory to choose environment
class SparkEnvironmentFactory:
    @staticmethod
    def get_spark_environment(is_databricks, is_local=False, **kwargs):
        if is_local:
            return LocalSparkEnvironment(
                local_spark_ui_url=kwargs.get("local_spark_ui_url"),
                local_application_id=kwargs.get("local_application_id")
            )
        elif is_databricks:
            return DatabricksSparkEnvironment(
                databricks_token=kwargs.get("databricks_token"), 
                databricks_instance=kwargs.get("databricks_instance"),
                databricks_application_id=kwargs.get("databricks_application_id")
            )
        else:
            return NonDatabricksSparkEnvironment(
                spark_master_url=kwargs.get("spark_master_url"),
                NonDatabricksSparkEnvironment_application_id=kwargs.get("NonDatabricksSparkEnvironment_application_id")
            )

def main():
    # Example: Choose environment
    is_databricks = False  # Or True if using Databricks Spark
    is_local = True  # For local Spark UI testing on localhost
    application_id = "local-1728540004669" # For giving application id (Mandatory to pass)

    if is_local:
        local_spark_ui_url = "http://localhost:4040/api/v1"
        local_application_id = application_id
        spark_env = SparkEnvironmentFactory.get_spark_environment(
            is_databricks=False, 
            is_local=True, 
            local_spark_ui_url=local_spark_ui_url,
            local_application_id = local_application_id
        )
    elif is_databricks:
        databricks_token = "YOUR_DATABRICKS_ACCESS_TOKEN"
        databricks_instance = "https://<databricks-instance>"
        databricks_application_id = application_id
        spark_env = SparkEnvironmentFactory.get_spark_environment(
            is_databricks=True, 
            databricks_token=databricks_token, 
            databricks_instance=databricks_instance,
            databricks_application_id = databricks_application_id
        )
    else:
        spark_master_url = "http://<spark-master-url>:4040/api/v1"
        NonDatabricksSparkEnvironment_application_id = application_id
        spark_env = SparkEnvironmentFactory.get_spark_environment(
            is_databricks=False, 
            spark_master_url=spark_master_url,
            NonDatabricksSparkEnvironment_application_id = NonDatabricksSparkEnvironment_application_id
        )

    jobId = 22 # For providing a specific job id

    # Get all the job details of the application
    all_job_details_of_application = spark_env.get_all_job_details_of_application()

    # Get all job ids of the application
    all_job_ids = [jobs.get("jobId", None) for jobs in all_job_details_of_application]
    all_job_ids = [all_job_ids[0]]
    
    specific_job_details = spark_env.get_specific_job_details(job_id = jobId)
    all_job_ids = [jobId]
    print(all_job_ids)

    #  Get all the stage details of all the jobs of the application
    all_jobs_stage_details = [spark_env.get_stage_details(job_id = jobId) for jobId in all_job_ids]

    # Get all the stage ids of all the jobs of the application
    all_stage_ids =  []
    for single_stage_details in all_jobs_stage_details:
        for stage in single_stage_details.get("stages"):
            all_stage_ids.append(stage[0].get("stageId", None))

    # Get all the stage and attempt ids (mapped) of all the jobs of the application
    all_stage_and_attempt_ids = [] 
    for single_stage_details in all_jobs_stage_details:
        for stage in single_stage_details.get("stages"):
            all_stage_and_attempt_ids.append({"stageId" : stage[0].get("stageId", None), "attemptId" : stage[0].get("attemptId", None)})

    # Get all the task details of all the stages of all the jobs of the application
    all_task_details = [spark_env.get_task_details(stage_id = mappedIds.get("stageId"), stage_attempt_id = mappedIds.get("attemptId")) for mappedIds in all_stage_and_attempt_ids]
    
    return_json = {
        "applicationId" : application_id,
        # "allJobDetails" : all_job_details_of_application,
        "specificJobDetails" : specific_job_details,
        "stageDetails" : all_jobs_stage_details,
        "task_details" : all_task_details,
    }
    
    # Example OpenAI question
    question = """
    You are a pyspark and apache spark expert.
    I am going to provide you a json which is derived from spark ui matrix.
    I want you to use the data of the given json and provide me suggestions of how can I make my spark job get faster and more optimized.
    I want you to only focus on my given json data and suggest suggestions, don't give answers that is deviating from the data carried within the json.
    Be very specific and to the point to data and avoid providing general advices.
    Before you give any suggestion think carefully about the matrix from the given json.
    What ever suggestions you give be very precise and specific exactly what to do interms of technicality and don't be theoratic.
    The way of suggestion should be this way first tell the current issue, then tell what does this mean, then tell the resolution.
    """

    # Write the collected spark matrix in a json file
    with open('/Users/heaven-is-here/Desktop/Codes/Spark ui integration with Open AI/raw_responses/response.json', 'w') as f:
        f.write(json.dumps(return_json))
    
    # Remove un necessary data and make the json carry useful data
    formatted_data = spark_env.json_compaction(return_json)

    # Write the compacted json in a json file
    with open('/Users/heaven-is-here/Desktop/Codes/Spark ui integration with Open AI/compacted_responses/compacted_response_test.json', 'w') as f:
        f.write(json.dumps(return_json))
    
    # Write the open-ai's response in a text file
    with open("/Users/heaven-is-here/Desktop/Codes/Spark ui integration with Open AI/open_ai_response/open_ai_response.txt", "a") as f:
        f.write("\n\n\n" + "###################################################################" + 
                "\n" + f"Application_ID = {application_id}, Job_Id = {jobId} \n\n\n\n" +
                spark_env.ask_openai(question=question, formatted_data=formatted_data))
    
    

if __name__ == "__main__":
    main()