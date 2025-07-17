"""
Azure Batch Utilities for Prefect

This module provides Prefect tasks and utilities for submitting and monitoring 
Azure Batch jobs as part of Prefect flows.
"""

import os
import asyncio
import time
import uuid
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

import logging
from azure.batch import BatchServiceClient
from azure.batch.models import (
    JobAddParameter,
    TaskAddParameter,
    PoolInformation,
    TaskContainerSettings,
    EnvironmentSetting,
    ResourceFile,
    OutputFile,
    OutputFileDestination,
    OutputFileBlobContainerDestination,
    OutputFileUploadOptions,
    OutputFileUploadCondition,
    TaskState
)
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import ResourceNotFoundError

# Blah #2

async def submit_batch_job(
    job_name: str,
    container_image: str,
    command_line: str,
    pool_name: str = "alphapath-compute-pool",
    environment_variables: Optional[Dict[str, str]] = None,
    input_files: Optional[List[Dict[str, str]]] = None,
    output_files: Optional[List[Dict[str, str]]] = None,
    timeout_minutes: int = 15,  # COST PROTECTION: Reduced default timeout to 15 minutes
    max_timeout_minutes: int = 30  # COST PROTECTION: Hard limit of 30 minutes
) -> Dict[str, Any]:
    """
    Submit a containerized job to Azure Batch and monitor its completion.
    
    COST PROTECTION: Jobs are limited to 30 minutes maximum and automatically cleaned up.
    
    Args:
        job_name: Unique name for the batch job
        container_image: Container image to run (e.g., 'alphapathregistry.azurecr.io/batch-test:latest')
        command_line: Command to execute in the container
        pool_name: Name of the batch pool to use
        environment_variables: Environment variables to set in the container
        input_files: List of input files to download (format: [{"blob_name": "file.txt", "file_path": "/tmp/file.txt"}])
        output_files: List of output files to upload (format: [{"file_path": "/tmp/output.json", "blob_name": "output.json"}])
        timeout_minutes: Maximum time to wait for job completion (max 30 minutes)
        max_timeout_minutes: Hard timeout limit (30 minutes maximum)
        
    Returns:
        Dict containing job results, status, and metadata
    """
    logger = logging.getLogger(__name__)
    
    # COST PROTECTION: Enforce timeout limits
    timeout_minutes = min(timeout_minutes, max_timeout_minutes)
    if timeout_minutes > max_timeout_minutes:
        logger.warning(f"⚠️ Timeout reduced from {timeout_minutes} to {max_timeout_minutes} minutes for cost protection")
    
    # Create unique job ID outside try block to avoid UnboundLocalError
    job_id = f"{job_name}-{uuid.uuid4().hex[:8]}"
    task_id = f"task-{uuid.uuid4().hex[:8]}"
    
    try:
        # Initialize Azure Batch client
        batch_client = await _get_batch_client()
        
        logger.info(f"🚀 Submitting Azure Batch job: {job_id}")
        logger.info(f"💰 COST PROTECTION: Max timeout {timeout_minutes} minutes, auto-cleanup enabled")
        logger.info(f"Container image: {container_image}")
        logger.info(f"Command: {command_line}")
        
        # Create the job with timeout constraints
        job = await _create_batch_job(batch_client, job_id, pool_name, timeout_minutes)
        
        # Create the task with timeout constraints
        task = await _create_batch_task(
            batch_client, 
            job_id, 
            task_id, 
            container_image, 
            command_line,
            environment_variables,
            input_files,
            output_files,
            timeout_minutes
        )
        
        # Monitor job completion
        result = await _monitor_batch_job(batch_client, job_id, task_id, timeout_minutes)
        
        # COST PROTECTION: Always cleanup job after completion
        await cleanup_batch_job(job_id)
        
        logger.info(f"✅ Batch job {job_id} completed successfully and cleaned up")
        
        return {
            "status": "completed",
            "job_id": job_id,
            "task_id": task_id,
            "container_image": container_image,
            "timeout_minutes": timeout_minutes,
            "result": result
        }
        
    except Exception as e:
        logger.error(f"💥 Batch job failed: {str(e)}")
        # COST PROTECTION: Always attempt cleanup on failure
        try:
            await cleanup_batch_job(job_id)
            logger.info(f"🧹 Cleaned up failed job {job_id}")
        except:
            logger.warning(f"⚠️ Could not clean up job {job_id}")
        raise


async def _get_batch_client() -> BatchServiceClient:
    """Initialize Azure Batch client using managed identity with token wrapping"""
    try:
        # Get batch account details from environment
        batch_account_name = os.getenv("AZURE_BATCH_ACCOUNT", "alphapathbatch425f544e")
        batch_account_url = f"https://{batch_account_name}.northcentralus.batch.azure.com"
        
        # Use managed identity for authentication with token wrapping
        # This resolves the 'signed_session' compatibility issue between
        # azure-batch SDK and modern azure-identity
        credential = DefaultAzureCredential()
        
        # Get token for Azure Batch API
        token = credential.get_token("https://batch.core.windows.net/.default")
        
        # Wrap token in BasicTokenAuthentication for compatibility with azure-batch
        from msrest.authentication import BasicTokenAuthentication
        token_auth = BasicTokenAuthentication({"access_token": token.token})
        
        batch_client = BatchServiceClient(
            credentials=token_auth,
            batch_url=batch_account_url
        )
        
        return batch_client
    except Exception as e:
        raise RuntimeError(f"Failed to initialize Batch client: {e}")


async def _create_batch_job(batch_client: BatchServiceClient, job_id: str, pool_name: str, timeout_minutes: int = 30) -> None:
    """Create a new batch job with timeout constraints"""
    logger = logging.getLogger(__name__)
    
    try:
        pool_info = PoolInformation(pool_id=pool_name)
        
        # COST PROTECTION: Set job-level timeout
        max_wall_clock_time = timedelta(minutes=timeout_minutes)
        
        job = JobAddParameter(
            id=job_id,
            pool_info=pool_info,
            constraints={
                'max_wall_clock_time': max_wall_clock_time,
                'max_task_retry_count': 1  # COST PROTECTION: Limit retries
            }
        )
        
        batch_client.job.add(job)
        logger.info(f"📋 Created batch job: {job_id} (timeout: {timeout_minutes} minutes)")
        
    except Exception as e:
        raise RuntimeError(f"Failed to create batch job: {e}")


async def _create_batch_task(
    batch_client: BatchServiceClient,
    job_id: str,
    task_id: str,
    container_image: str,
    command_line: str,
    environment_variables: Optional[Dict[str, str]] = None,
    input_files: Optional[List[Dict[str, str]]] = None,
    output_files: Optional[List[Dict[str, str]]] = None,
    timeout_minutes: int = 30
) -> None:
    """Create a batch task with container settings"""
    logger = logging.getLogger(__name__)
    
    try:
        # Prepare environment variables
        env_settings = []
        default_env = {
            "AZURE_STORAGE_ACCOUNT": os.getenv("AZURE_STORAGE_ACCOUNT", "alphapathblobs425f544e"),
            "TASK_ID": task_id
        }
        
        if environment_variables:
            default_env.update(environment_variables)
        
        for key, value in default_env.items():
            env_settings.append(EnvironmentSetting(name=key, value=value))
        
        # Prepare resource files (input files to download)
        resource_files = []
        if input_files:
            for input_file in input_files:
                # Create blob URL for input file
                storage_account = os.getenv("AZURE_STORAGE_ACCOUNT", "alphapathblobs425f544e")
                blob_url = f"https://{storage_account}.blob.core.windows.net/input/{input_file['blob_name']}"
                
                resource_files.append(
                    ResourceFile(
                        http_url=blob_url,
                        file_path=input_file['file_path']
                    )
                )
        
        # Prepare output files (files to upload after task completion)
        output_file_list = []
        if output_files:
            storage_account = os.getenv("AZURE_STORAGE_ACCOUNT", "alphapathblobs425f544e")
            
            for output_file in output_files:
                output_file_list.append(
                    OutputFile(
                        file_pattern=output_file['file_path'],
                        destination=OutputFileDestination(
                            container=OutputFileBlobContainerDestination(
                                container_url=f"https://{storage_account}.blob.core.windows.net/output",
                                path=output_file['blob_name']
                            )
                        ),
                        upload_options=OutputFileUploadOptions(
                            upload_condition=OutputFileUploadCondition.task_completion
                        )
                    )
                )
        
        # Container settings
        container_settings = TaskContainerSettings(
            image_name=container_image,
            container_run_options="--rm"
        )
        
        # Create the task
        task = TaskAddParameter(
            id=task_id,
            command_line=command_line,
            container_settings=container_settings,
            environment_settings=env_settings,
            resource_files=resource_files,
            output_files=output_file_list
        )
        
        batch_client.task.add(job_id=job_id, task=task)
        logger.info(f"📄 Created batch task: {task_id}")
        
    except Exception as e:
        raise RuntimeError(f"Failed to create batch task: {e}")


async def _monitor_batch_job(
    batch_client: BatchServiceClient, 
    job_id: str, 
    task_id: str, 
    timeout_minutes: int
) -> Dict[str, Any]:
    """Monitor batch job completion and return results"""
    logger = logging.getLogger(__name__)
    
    start_time = datetime.now()
    timeout = timedelta(minutes=timeout_minutes)
    
    logger.info(f"⏳ Monitoring batch job {job_id} (timeout: {timeout_minutes} minutes)")
    
    try:
        while datetime.now() - start_time < timeout:
            try:
                # Get task status
                task = batch_client.task.get(job_id, task_id)
                
                # Get task state and convert to string for logging
                task_state_str = str(task.state)
                logger.info(f"📊 Task state: {task.state} ({task_state_str})")
                
                # Compare against actual TaskState enum values for reliability
                if task.state == TaskState.completed:
                    # Task completed successfully
                    execution_info = task.execution_info
                    logger.info(f"✅ Task completed with exit code: {execution_info.exit_code}")
                    
                    # Get task output (stdout/stderr)
                    stdout, stderr = await _get_task_output(batch_client, job_id, task_id)
                    
                    return {
                        "state": task_state_str,
                        "exit_code": execution_info.exit_code,
                        "start_time": execution_info.start_time.isoformat() if execution_info.start_time else None,
                        "end_time": execution_info.end_time.isoformat() if execution_info.end_time else None,
                        "stdout": stdout,
                        "stderr": stderr
                    }
                
                elif task.state not in [TaskState.active, TaskState.preparing, TaskState.running, TaskState.completed]:
                    # Task is in an unknown/error state
                    execution_info = task.execution_info
                    logger.error(f"❌ Task in unknown state: {task.state}")
                    
                    if execution_info and execution_info.exit_code is not None:
                        logger.error(f"❌ Task failed with exit code: {execution_info.exit_code}")
                        stdout, stderr = await _get_task_output(batch_client, job_id, task_id)
                        raise RuntimeError(f"Batch task failed with exit code {execution_info.exit_code}")
                    else:
                        raise RuntimeError(f"Batch task in unknown state: {task.state}")
                
                elif task.state in [TaskState.active, TaskState.preparing, TaskState.running]:
                    # Task still in progress - provide more details
                    logger.info(f"⏳ Task still in progress, waiting 10 seconds...")
                    
                    # Log additional task details for debugging
                    if hasattr(task, 'node_info') and task.node_info:
                        logger.info(f"🖥️ Task running on node: {task.node_info.node_id}")
                    
                    if hasattr(task, 'execution_info') and task.execution_info:
                        exec_info = task.execution_info
                        if exec_info.start_time:
                            logger.info(f"⏱️ Task started at: {exec_info.start_time}")
                        if exec_info.exit_code is not None:
                            logger.info(f"🔢 Exit code: {exec_info.exit_code}")
                    
                    await asyncio.sleep(10)
                
                else:
                    # Unknown state
                    logger.warning(f"⚠️ Unknown task state: {task.state}, continuing to monitor...")
                    await asyncio.sleep(10)
                
            except ResourceNotFoundError:
                logger.warning(f"Task {task_id} not found, retrying...")
                await asyncio.sleep(5)
                continue
        
        # Timeout exceeded
        raise TimeoutError(f"Batch job {job_id} timed out after {timeout_minutes} minutes")
        
    except Exception as e:
        logger.error(f"Error monitoring batch job: {e}")
        raise


async def _get_task_output(batch_client: BatchServiceClient, job_id: str, task_id: str) -> tuple:
    """Get stdout and stderr from completed task"""
    try:
        stdout_content = ""
        stderr_content = ""
        
        try:
            stdout_stream = batch_client.file.get_from_task(job_id, task_id, "stdout.txt")
            stdout_content = stdout_stream.content.decode('utf-8')
        except ResourceNotFoundError:
            stdout_content = "No stdout available"
        
        try:
            stderr_stream = batch_client.file.get_from_task(job_id, task_id, "stderr.txt")
            stderr_content = stderr_stream.content.decode('utf-8')
        except ResourceNotFoundError:
            stderr_content = "No stderr available"
        
        return stdout_content, stderr_content
        
    except Exception as e:
        return f"Error getting stdout: {e}", f"Error getting stderr: {e}"


async def cleanup_batch_job(job_id: str) -> bool:
    """Clean up a completed batch job"""
    logger = logging.getLogger(__name__)
    
    try:
        batch_client = await _get_batch_client()
        
        # Delete the job (this also deletes associated tasks)
        batch_client.job.delete(job_id)
        logger.info(f"🧹 Cleaned up batch job: {job_id}")
        
        return True
        
    except Exception as e:
        logger.warning(f"Failed to cleanup batch job {job_id}: {e}")
        return False 