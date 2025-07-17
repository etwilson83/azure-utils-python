# Azure Utils

Azure utilities for AlphaPath biomedical image processing pipeline.

## Installation

### From Git Repository
```bash
pip install git+https://github.com/your-org/azure-utils.git
```

### For Development
```bash
git clone https://github.com/your-org/azure-utils.git
cd azure-utils
pip install -e .
```

## Usage

```python
from azure_utils import (
    submit_batch_job, 
    cleanup_batch_job,
    run_aci_container,
    list_input_images,
    check_processing_status,
    AzureBatchMonitor
)

# Submit a batch job
result = await submit_batch_job(
    job_id="my-job", 
    task_id="my-task",
    # ... other parameters
)

# Run ACI container
success = await run_aci_container(
    image="myimage:latest",
    container_name="my-container"
)
```

## Requirements

This package requires Python 3.8+ and the following dependencies:
- azure-batch>=14.0.0
- azure-storage-blob>=12.0.0
- azure-identity>=1.12.0
- azure-mgmt-containerinstance>=10.0.0
- prefect>=3.0.0