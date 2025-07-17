"""
Azure Container Instance utilities for Prefect orchestration
"""
import logging
import time
import asyncio
import os
from typing import Dict, Optional

# Azure configuration constants
SUBSCRIPTION_ID = os.getenv("AZURE_SUBSCRIPTION_ID")
RESOURCE_GROUP = os.getenv("AZURE_RESOURCE_GROUP", "alphapath-rg")
LOCATION = "eastus"
STORAGE_ACCOUNT = "alphapath"

# Build managed identity path dynamically from environment variables
if SUBSCRIPTION_ID:
    MANAGED_IDENTITY = f"/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/alphapath-aca-identity"
else:
    MANAGED_IDENTITY = None  # Will be set when SUBSCRIPTION_ID is available

def _lazy_import_azure():
    """Lazy import Azure modules only when needed"""
    try:
        from azure.mgmt.containerinstance import ContainerInstanceManagementClient
        from azure.identity import DefaultAzureCredential
        from azure.mgmt.containerinstance.models import (
            ContainerGroup, Container, ContainerGroupIdentity, ResourceRequirements, 
            ResourceRequests, Volume, AzureFileVolume, VolumeMount, ContainerGroupRestartPolicy,
            EnvironmentVariable, ImageRegistryCredential
        )
        return {
            'ContainerInstanceManagementClient': ContainerInstanceManagementClient,
            'DefaultAzureCredential': DefaultAzureCredential,
            'ContainerGroup': ContainerGroup,
            'Container': Container,
            'ContainerGroupIdentity': ContainerGroupIdentity,
            'ResourceRequirements': ResourceRequirements,
            'ResourceRequests': ResourceRequests,
            'Volume': Volume,
            'AzureFileVolume': AzureFileVolume,
            'VolumeMount': VolumeMount,
            'ContainerGroupRestartPolicy': ContainerGroupRestartPolicy,
            'EnvironmentVariable': EnvironmentVariable,
            'ImageRegistryCredential': ImageRegistryCredential,
        }
    except ImportError as e:
        raise ImportError(
            f"Azure SDK packages are required but not installed: {e}. "
            "Please install them with: pip install azure-mgmt-containerinstance azure-identity azure-mgmt-resource"
        )

async def run_aci_container(
    image: str,
    container_name: str,
    environment_vars: Optional[Dict[str, str]] = None,
    command: Optional[list] = None,
    cpu: float = 1.0,
    memory: float = 2.0,
    timeout_minutes: int = 60
) -> bool:
    """
    Create, run, and monitor an Azure Container Instance
    
    Args:
        image: Container image to run (e.g., 'spiregistry.azurecr.io/histoqc:latest')
        container_name: Unique name for the container instance
        environment_vars: Optional environment variables to set
        command: Optional command to override container default (e.g., ["python3", "process.py", "/shared-data/input"])
        cpu: CPU allocation (cores)
        memory: Memory allocation (GB)
        timeout_minutes: Maximum time to wait for completion
        
    Returns:
        bool: True if container completed successfully, False otherwise
    """
    logger = logging.getLogger(__name__)
    
    # Lazy import Azure modules
    azure_modules = _lazy_import_azure()
    
    # Check for required environment variables
    subscription_id = SUBSCRIPTION_ID or os.getenv("AZURE_SUBSCRIPTION_ID")
    if not subscription_id:
        raise ValueError("AZURE_SUBSCRIPTION_ID environment variable is required")
    
    managed_identity = MANAGED_IDENTITY or f"/subscriptions/{subscription_id}/resourceGroups/{RESOURCE_GROUP}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/alphapath-aca-identity"
    
    # Initialize Azure client
    credential = azure_modules['DefaultAzureCredential']()
    aci_client = azure_modules['ContainerInstanceManagementClient'](credential, subscription_id)
    
    # Prepare environment variables
    env_vars = []
    
    # Start with defaults
    default_env_vars = {
        "INPUT_DIR": "/shared-data/input",
        "OUTPUT_DIR": "/shared-data/output", 
        "SHARED_DATA_DIR": "/shared-data"
    }
    
    # Override defaults with any custom environment variables
    if environment_vars:
        default_env_vars.update(environment_vars)
    
    # Convert to Azure EnvironmentVariable objects
    for key, value in default_env_vars.items():
        env_vars.append(azure_modules['EnvironmentVariable'](name=key, value=value))
    
    # Define the container group
    container_group = azure_modules['ContainerGroup'](
        location=LOCATION,
        containers=[
            azure_modules['Container'](
                name=container_name,
                image=image,
                command=command,
                resources=azure_modules['ResourceRequirements'](
                    requests=azure_modules['ResourceRequests'](memory_in_gb=memory, cpu=cpu)
                ),
                volume_mounts=[
                    azure_modules['VolumeMount'](name="shared-data", mount_path="/shared-data")
                ],
                environment_variables=env_vars
            )
        ],
        volumes=[
            azure_modules['Volume'](
                name="shared-data",
                azure_file=azure_modules['AzureFileVolume'](
                    share_name="shared-data",
                    storage_account_name=STORAGE_ACCOUNT,
                    storage_account_key=os.getenv("AZURE_STORAGE_KEY")
                )
            )
        ],
        restart_policy=azure_modules['ContainerGroupRestartPolicy'].never,
        os_type="Linux",
        identity=azure_modules['ContainerGroupIdentity'](
            type="UserAssigned",
            user_assigned_identities={managed_identity: {}}
        ),
        image_registry_credentials=[
            azure_modules['ImageRegistryCredential'](
                server="alphapathregistry.azurecr.io",
                identity=managed_identity
            )
        ]
    )
    
    try:
        # Create the container group
        logger.info(f"Creating container: {container_name} with image: {image}")
        creation_result = aci_client.container_groups.begin_create_or_update(
            RESOURCE_GROUP, container_name, container_group
        )
        
        # Wait for creation to complete
        container_group_result = creation_result.result()
        logger.info(f"Container created successfully: {container_group_result.name}")
        
        # Monitor container execution
        success = await _monitor_container_completion(aci_client, container_name, timeout_minutes)
        
        return success
        
    except Exception as e:
        logger.error(f"Failed to create/run container {container_name}: {e}")
        return False
    finally:
        # Cleanup
        try:
            logger.info(f"Cleaning up container: {container_name}")
            aci_client.container_groups.begin_delete(RESOURCE_GROUP, container_name)
        except Exception as e:
            logger.warning(f"Failed to cleanup container {container_name}: {e}")

async def _monitor_container_completion(aci_client, container_name: str, timeout_minutes: int) -> bool:
    """Monitor container until completion or timeout"""
    logger = logging.getLogger(__name__)
    
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    log_lines_seen = set()  # Track which log lines we've already shown
    
    logger.info(f"🔍 Monitoring container: {container_name} (timeout: {timeout_minutes} minutes)")
    logger.info(f"Real-time log streaming enabled...")
    
    consecutive_log_failures = 0
    last_state_log_time = 0
    
    while True:
        current_time = time.time()
        elapsed = current_time - start_time
        
        # Check timeout
        if elapsed > timeout_seconds:
            logger.error(f"⏱️ Container {container_name} timed out after {timeout_minutes} minutes")
            return False
        
        try:
            container_group = aci_client.container_groups.get(RESOURCE_GROUP, container_name)
            
            if not container_group.containers or not container_group.containers[0].instance_view:
                logger.info("📦 Container initializing...")
                await asyncio.sleep(5)
                continue
                
            current_state = container_group.containers[0].instance_view.current_state
            state = current_state.state if current_state else "Unknown"
            
            # Stream container logs in real-time with better error handling
            try:
                logs_response = aci_client.containers.list_logs(
                    RESOURCE_GROUP, container_name, container_name
                )
                if logs_response.content:
                    # Process all log lines and track which ones we've seen
                    all_lines = logs_response.content.split('\n')
                    
                    new_log_count = 0
                    for i, line in enumerate(all_lines):
                        line_key = f"{i}:{line.strip()}"
                        if line.strip() and line_key not in log_lines_seen:
                            logger.info(f"[Container] {line.strip()}")
                            log_lines_seen.add(line_key)
                            new_log_count += 1
                    
                    if new_log_count > 0:
                        consecutive_log_failures = 0  # Reset failure counter
                        logger.debug(f"Streamed {new_log_count} new log lines")
                
            except Exception as e:
                consecutive_log_failures += 1
                if consecutive_log_failures <= 3:  # Only log first few failures
                    logger.warning(f"Log fetch attempt {consecutive_log_failures} failed: {e}")
                elif consecutive_log_failures == 10:
                    logger.error("Multiple log fetch failures - container may have issues")
            
            # Log container state periodically (every 30 seconds) instead of every poll
            if current_time - last_state_log_time > 30:
                logger.info(f"Container state: {state} (elapsed: {elapsed:.1f}s)")
                last_state_log_time = current_time
            
            if state == "Terminated":
                exit_code = current_state.exit_code
                
                # Get final logs with retry
                logger.info(f"=== FINAL LOGS FROM {container_name} ===")
                try:
                    final_logs = aci_client.containers.list_logs(RESOURCE_GROUP, container_name, container_name)
                    if final_logs.content:
                        final_lines = final_logs.content.split('\n')
                        # Show all final logs to ensure nothing is missed
                        for line in final_lines:
                            if line.strip():
                                logger.info(f"[Final] {line.strip()}")
                    else:
                        logger.warning("No final logs available")
                except Exception as e:
                    logger.error(f"Could not retrieve final logs: {e}")
                finally:
                    logger.info(f"=== END LOGS FROM {container_name} ===")
                
                if exit_code == 0:
                    logger.info(f"✅ Container {container_name} completed successfully (exit code: 0)")
                    return True
                else:
                    logger.error(f"❌ Container {container_name} failed with exit code: {exit_code}")
                    return False
                    
            elif state == "Failed":
                logger.error(f"❌ Container {container_name} failed")
                
                # Get failure logs
                try:
                    failure_logs = aci_client.containers.list_logs(RESOURCE_GROUP, container_name, container_name)
                    if failure_logs.content:
                        logger.error(f"=== FAILURE LOGS FROM {container_name} ===")
                        for line in failure_logs.content.split('\n'):
                            if line.strip():
                                logger.error(f"[{container_name}] {line}")
                        logger.error(f"=== END FAILURE LOGS FROM {container_name} ===")
                except:
                    pass
                    
                return False
                
            # Check timeout
            if time.time() - start_time > timeout_seconds:
                logger.error(f"Container {container_name} timed out after {timeout_minutes} minutes")
                
                # Get timeout logs
                try:
                    timeout_logs = aci_client.containers.list_logs(RESOURCE_GROUP, container_name, container_name)
                    if timeout_logs.content:
                        logger.warning(f"=== TIMEOUT LOGS FROM {container_name} ===")
                        for line in timeout_logs.content.split('\n'):
                            if line.strip():
                                logger.warning(f"[{container_name}] {line}")
                        logger.warning(f"=== END TIMEOUT LOGS FROM {container_name} ===")
                except:
                    pass
                    
                return False
                
            await asyncio.sleep(5)  # Check every 5 seconds for more responsive logging
            
        except Exception as e:
            logger.error(f"Error monitoring container {container_name}: {e}")
            return False 