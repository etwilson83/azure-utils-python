"""
Azure Blob Storage utilities for AlphaPath pipeline
"""
import os
from typing import List, Dict, Optional, Set
from prefect import task, get_run_logger
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from pathlib import Path

# Storage configuration
STORAGE_ACCOUNT_NAME = "alphapathblobs"  # From terraform config
INPUT_CONTAINER = "input"
OUTPUT_CONTAINER = "output"
ARCHIVE_CONTAINER = "archive"

def get_blob_service_client() -> BlobServiceClient:
    """Get authenticated blob service client using managed identity"""
    account_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
    credential = DefaultAzureCredential()
    return BlobServiceClient(account_url=account_url, credential=credential)

@task(name="List Input Images")
async def list_input_images() -> List[Dict[str, str]]:
    """
    List all .svs files in the input blob container.
    
    Returns:
        List of dicts with image metadata: [{"name": "image.svs", "path": "input/image.svs", "size": 1234567}]
    """
    logger = get_run_logger()
    logger.info(f"🔍 Scanning {INPUT_CONTAINER} container for .svs files...")
    
    blob_client = get_blob_service_client()
    container_client = blob_client.get_container_client(INPUT_CONTAINER)
    
    images = []
    
    try:
        # List all blobs in the input container
        blobs = container_client.list_blobs()
        
        for blob in blobs:
            # Only include .svs files
            if blob.name.lower().endswith('.svs'):
                images.append({
                    "name": blob.name,
                    "path": f"{INPUT_CONTAINER}/{blob.name}",
                    "size": blob.size,
                    "last_modified": blob.last_modified.isoformat() if blob.last_modified else None
                })
        
        logger.info(f"✅ Found {len(images)} .svs files in {INPUT_CONTAINER}")
        for img in images:
            logger.info(f"  - {img['name']} ({img['size'] / (1024*1024):.1f} MB)")
            
    except Exception as e:
        logger.error(f"❌ Failed to list images from {INPUT_CONTAINER}: {e}")
        raise
    
    return images

@task(name="Check Processing Status")  
async def check_processing_status(image_list: List[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    """
    Check which images have been processed by looking for outputs in the output container.
    
    Args:
        image_list: List of image metadata from list_input_images()
        
    Returns:
        Dict with "pending" and "processed" lists of image metadata
    """
    logger = get_run_logger()
    logger.info(f"🔍 Checking processing status for {len(image_list)} images...")
    
    blob_client = get_blob_service_client()
    output_container_client = blob_client.get_container_client(OUTPUT_CONTAINER)
    
    # Get list of all processed image directories in output container
    processed_image_names = set()
    
    try:
        # List all blobs in output container to find processed images
        output_blobs = output_container_client.list_blobs()
        
        for blob in output_blobs:
            # Extract image name from output path structure
            # Expected structure: output/image_name/... (from HistoQC processing)
            path_parts = blob.name.split('/')
            if len(path_parts) > 1:
                image_dir = path_parts[0]  # First directory level is the image name
                processed_image_names.add(image_dir)
        
        logger.info(f"Found outputs for {len(processed_image_names)} processed images")
        
    except Exception as e:
        logger.warning(f"Could not check output container: {e}")
        # If we can't check outputs, assume all need processing
        processed_image_names = set()
    
    # Categorize images
    pending = []
    processed = []
    
    for image in image_list:
        # Get image name without extension for comparison
        image_name = Path(image["name"]).stem
        
        if image_name in processed_image_names:
            processed.append(image)
            logger.info(f"✅ Already processed: {image['name']}")
        else:
            pending.append(image)
            logger.info(f"⏳ Needs processing: {image['name']}")
    
    result = {
        "pending": pending,
        "processed": processed
    }
    
    logger.info(f"📊 Processing status: {len(pending)} pending, {len(processed)} already processed")
    
    return result

@task(name="Download Image from Blob")
async def download_image_from_blob(image_name: str, local_path: str) -> str:
    """
    Download an image from the input blob container to local storage.
    
    Args:
        image_name: Name of the image file in the input container
        local_path: Local path where the image should be saved
        
    Returns:
        str: Local file path of the downloaded image
    """
    logger = get_run_logger()
    logger.info(f"⬇️ Downloading {image_name} to {local_path}")
    
    blob_client = get_blob_service_client()
    blob_client_for_file = blob_client.get_blob_client(
        container=INPUT_CONTAINER,
        blob=image_name
    )
    
    try:
        # Ensure local directory exists
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        # Download the blob
        with open(local_path, "wb") as download_file:
            download_stream = blob_client_for_file.download_blob()
            download_file.write(download_stream.readall())
        
        # Verify download
        file_size = os.path.getsize(local_path)
        logger.info(f"✅ Downloaded {image_name} ({file_size / (1024*1024):.1f} MB)")
        
        return local_path
        
    except Exception as e:
        logger.error(f"❌ Failed to download {image_name}: {e}")
        raise

@task(name="Upload Results to Blob")
async def upload_results_to_blob(local_output_dir: str, image_name: str) -> Dict[str, str]:
    """
    Upload processing results from local directory to output blob container.
    
    Args:
        local_output_dir: Local directory containing processing results
        image_name: Name of the processed image (for organizing outputs)
        
    Returns:
        Dict with upload summary: {"uploaded_files": count, "blob_prefix": "output/image_name/"}
    """
    logger = get_run_logger()
    image_base_name = Path(image_name).stem  # Remove .svs extension
    blob_prefix = f"{image_base_name}/"
    
    logger.info(f"⬆️ Uploading results from {local_output_dir} to {OUTPUT_CONTAINER}/{blob_prefix}")
    
    blob_client = get_blob_service_client()
    container_client = blob_client.get_container_client(OUTPUT_CONTAINER)
    
    uploaded_count = 0
    
    try:
        # Walk through all files in the output directory
        for root, dirs, files in os.walk(local_output_dir):
            for file in files:
                local_file_path = os.path.join(root, file)
                
                # Create relative path for blob storage
                relative_path = os.path.relpath(local_file_path, local_output_dir)
                blob_name = f"{blob_prefix}{relative_path}".replace(os.sep, '/')
                
                # Upload file to blob storage
                blob_client_for_file = container_client.get_blob_client(blob_name)
                
                with open(local_file_path, "rb") as data:
                    blob_client_for_file.upload_blob(data, overwrite=True)
                
                uploaded_count += 1
                logger.info(f"  ✅ Uploaded: {blob_name}")
        
        logger.info(f"✅ Upload complete: {uploaded_count} files uploaded to {OUTPUT_CONTAINER}/{blob_prefix}")
        
        return {
            "uploaded_files": uploaded_count,
            "blob_prefix": f"{OUTPUT_CONTAINER}/{blob_prefix}",
            "status": "success"
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to upload results: {e}")
        raise

@task(name="Archive Processed Image")  
async def archive_processed_image(image_name: str) -> Dict[str, str]:
    """
    Move a processed image from input to archive container.
    
    Args:
        image_name: Name of the image file to archive
        
    Returns:
        Dict with archive status
    """
    logger = get_run_logger()
    logger.info(f"📦 Archiving processed image: {image_name}")
    
    blob_client = get_blob_service_client()
    
    try:
        # Source blob in input container
        source_blob_client = blob_client.get_blob_client(
            container=INPUT_CONTAINER,
            blob=image_name
        )
        
        # Destination blob in archive container  
        dest_blob_client = blob_client.get_blob_client(
            container=ARCHIVE_CONTAINER,
            blob=image_name
        )
        
        # Copy from input to archive
        source_url = source_blob_client.url
        dest_blob_client.start_copy_from_url(source_url)
        
        # Wait for copy to complete (for small files this should be instant)
        copy_properties = dest_blob_client.get_blob_properties()
        if copy_properties.copy.status == "success":
            # Delete from input container
            source_blob_client.delete_blob()
            logger.info(f"✅ Archived {image_name} from {INPUT_CONTAINER} to {ARCHIVE_CONTAINER}")
            
            return {
                "status": "success",
                "archive_path": f"{ARCHIVE_CONTAINER}/{image_name}"
            }
        else:
            logger.error(f"❌ Copy failed for {image_name}: {copy_properties.copy.status}")
            return {
                "status": "failed",
                "error": f"Copy status: {copy_properties.copy.status}"
            }
            
    except Exception as e:
        logger.error(f"❌ Failed to archive {image_name}: {e}")
        return {
            "status": "failed", 
            "error": str(e)
        } 