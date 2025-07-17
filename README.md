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

## Cross-Repository Dependencies

This package automatically triggers rebuilds of dependent projects when changes are pushed.

### Setup for Private Repositories

**IMPORTANT:** For private repositories, you need a Personal Access Token (PAT) with `repo` scope.

#### 1. Create Personal Access Token
1. Go to GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic)
2. Generate new token (classic) with `repo` scope
3. Copy the token immediately

#### 2. Add Token to Repository Secrets
1. Go to `azure_utils` repository → Settings → Secrets and variables → Actions
2. Add new secret: `CROSS_REPO_TOKEN` with your PAT value

#### 3. Workflow Configuration
The workflow automatically sends repository_dispatch events to dependent repositories:
- `etwilson83/alphapath-jobs` (prefect-tasks)

#### 4. Event Types
- `azure_utils_updated` - Triggers when azure_utils code changes
- Excludes documentation changes (`.md`, `docs/`, etc.)

### Troubleshooting
- **404 Error**: Check repository name and PAT permissions
- **403 Error**: Ensure PAT has `repo` scope
- **No triggers**: Verify dependent repository has repository_dispatch listener

## Requirements

This package requires Python 3.8+ and the following dependencies:
- azure-batch>=14.0.0
- azure-storage-blob>=12.0.0
- azure-identity>=1.12.0
- azure-mgmt-containerinstance>=10.0.0
- prefect>=3.0.0