from setuptools import setup, find_packages

setup(
    name="fhir-patient-pipeline",
    version="0.1.0",
    description="End-to-end FHIR R4 patient data pipeline with lakehouse architecture",
    author="Data Engineering",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.11",
    install_requires=[
        "fhir.resources>=7.0.0",
        "duckdb>=0.9.0",
        "pandas>=2.0.0",
        "pyarrow>=14.0.0",
        "pyyaml>=6.0",
        "python-dateutil>=2.8.0",
        "jsonschema>=4.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
        ]
    },
)
