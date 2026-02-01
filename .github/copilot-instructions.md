# Copilot Instructions for CITO Project

## Overview
This document provides essential guidelines for AI coding agents working within the CITO project. Understanding the architecture, workflows, and conventions is crucial for effective contributions.

## Project Architecture
- **Main Components**: The project is structured around a pipeline that processes legal case data. Key components include:
  - **Pipeline Orchestration**: Managed by `step00-run-pipeline.py`, which coordinates the execution of various processing steps.
  - **Data Storage**: Utilizes MongoDB for storing case data, configured in `config/mongo.json`.
  - **AI Models**: Configurations for AI models are found in `config/ai-model.json`, detailing providers and API keys.

## Developer Workflows
- **Running the Pipeline**: Execute the pipeline using the command:
  ```bash
  python3 versions/development/poc-v-d33/core/step00-run-pipeline.py
  ```
  - Use the `--case-url` option to override case URLs if necessary.
- **Testing and Debugging**: Ensure to check logs for errors during pipeline execution. Each step logs its progress and errors.

## Project Conventions
- **File Naming**: Follow the naming conventions for scripts in the `core` directory, which indicate their processing order (e.g., `step02-get-case-html.py`).
- **Configuration Management**: All configurations are stored in JSON format within the `config` directory. Ensure to validate configurations before running the pipeline.

## Integration Points
- **External Dependencies**: The project relies on `pymongo` for MongoDB interactions. Ensure this package is installed in your environment.
- **Cross-Component Communication**: Data flows through the pipeline steps, with each step receiving input from the previous one. Ensure to handle data formats consistently across scripts.

## Examples
- **Example of Running a Step**: To run a specific step, use:
  ```bash
  python3 versions/development/poc-v-d33/core/step02-get-case-html.py <stfDecisionId>
  ```

## Conclusion
Familiarize yourself with the above components and workflows to effectively contribute to the CITO project. For any questions, refer to the project documentation or reach out to the team.