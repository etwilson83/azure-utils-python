from setuptools import setup, find_packages

setup(
    name="azure-utils",
    version="0.1.0",
    description="Azure utilities for AlphaPath biomedical image processing pipeline",
    author="AlphaPath Team",
    packages=find_packages(),
    install_requires=[
        "azure-batch>=14.0.0",
        "azure-storage-blob>=12.0.0",
        "azure-identity>=1.12.0",
        "azure-mgmt-containerinstance>=10.0.0",
        "azure-mgmt-resource>=21.0.0",
        "prefect>=3.0.0",
        "asyncio",
        "datetime",
        "typing",
        "enum34; python_version<'3.4'",
    ],
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
)