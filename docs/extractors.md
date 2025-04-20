# Data Extractors

This section documents the data extraction components within the workflow system.

## Overview

Data extraction is handled by classes inheriting from `BaseExtractor`. A key implementation provided is the `SlingExtractor`, which leverages the powerful `sling-python` library to connect to a wide variety of sources (APIs, databases, filesystems, etc.).

For source-specific logic, transformation, or configuration, specialized extractors are created by inheriting from `SlingExtractor`.

## SlingExtractor (`src/data_warehouse/extractors/sling_extractor.py`)

The `SlingExtractor` serves as a base for connecting to sources supported by `sling-python`.

### Configuration

`SlingExtractor` requires a configuration dictionary containing a `sling_config` key. This `sling_config` dictionary is passed directly to the `sling.Sling()` instance. Refer to the [sling-python documentation](https://slingdata-io.github.io/sling-python/sling.html#sling) for all available options.

```python
# Example base configuration structure
config = {
    "name": "my_sling_instance",
    "sling_config": {
        "source": "...",
        "source_options": { ... },
        # ... other sling options like format, timeout, retries ...
    }
    # ... other non-sling config keys ...
}
```

### Source-Specific Implementations

To handle specific APIs or data sources with custom requirements (e.g., specific authentication, pagination, data transformation), create a new class inheriting from `SlingExtractor`.

**Example: `NightscoutExtractor` (`src/data_warehouse/workflows/nightscout/extraction.py`)**

This extractor fetches glucose data from a Nightscout API.

1.  **Inheritance:** `class NightscoutExtractor(SlingExtractor): ...`
2.  **`__init__`:**
    - Accepts its own configuration parameters (`base_url`, `api_secret`, `lookback_hours`).
    - Constructs the specific `sling_config` needed for the Nightscout API endpoint, including authentication headers and query parameters based on its own config.
    - Merges this specific config with any base `sling_config` provided in the main configuration dictionary.
    - Calls `super().__init__(...)` with the final merged configuration.
    - **Crucially**, it explicitly sets `self._sling_config` to the _final merged config_ after the `super` call to ensure the correct settings are used by the base class methods.
3.  **`extract`:**
    - Calls `sling.Sling(**self._sling_config).stream()` to get raw data.
    - Transforms the list of dictionaries returned by Sling into the Pydantic model `GlucoseData`.
    - Includes specific validation and error handling for Nightscout data fields (`dateString`, `sgv`).

### Configuration Templates (`config/workflows/extractors/`)

Extractor instances are typically configured using YAML files within the `config/workflows/extractors/` directory (or a similar structured path). These templates are loaded and parsed by the `WorkflowManager`.

**Example: `nightscout.yaml`**

```yaml
---
# Example configuration template for NightscoutExtractor

extractors:
  - name: nightscout_glucose_hourly # Unique name for this extractor instance
    type: NightscoutExtractor # The Python class name
    config:
      # Nightscout-specific configuration
      base_url: ${NIGHTSCOUT_BASE_URL} # Use environment variable or provide directly
      api_secret: ${NIGHTSCOUT_API_SECRET} # Use environment variable or provide directly
      lookback_hours: 1 # Example: Fetch last 1 hour


      # Optional base Sling configuration (merged in __init__)
      # sling_config:
      #   timeout: 60
      #   retries: 3
```

This template defines an extractor named `nightscout_glucose_hourly` which will instantiate the `NightscoutExtractor` class using the provided `config`.

## Adding a New Extractor

1.  **Create Python Class:**
    - Create a new Python file (e.g., `src/data_warehouse/workflows/my_source/extraction.py`).
    - Define a class inheriting from `SlingExtractor` (or `BaseExtractor` if Sling is not used).
    - Implement `__init__(self, name: str, config: dict | None = None)`.
      - Validate specific `config` keys needed for your source.
      - If using `SlingExtractor`:
        - Prepare the source-specific `sling_config` dictionary.
        - Merge with any base `sling_config` from the input `config`.
        - Prepare the `config_for_base` dictionary containing the final merged `sling_config` under the `sling_config` key.
        - Call `super().__init__(name, config_for_base)`.
        - **Set `self._sling_config = final_merged_sling_config`**.
    - Implement/Override `extract(self) -> YourDataType`:
      - If using `SlingExtractor`, call `sling.Sling(**self._sling_config).stream()`.
      - Perform any necessary data transformation/validation.
      - Return data in the desired format (e.g., a Pydantic model).
    - Optionally override `validate_source(self)` or `get_metadata(self)`.
2.  **Create Configuration Template:**
    - Create a YAML file (e.g., `config/workflows/extractors/my_source.yaml`).
    - Define an extractor instance with a unique `name`, the `type` (your new class name), and the required `config` dictionary. Use environment variables (`${VAR_NAME}`) for sensitive values like API keys.
3.  **Discovery:** Ensure your new Python file is placed within a directory structure that the workflow discovery mechanism scans (typically within `src/data_warehouse/workflows/` or `src/data_warehouse/extractors/`).

## Monitoring

The `SlingExtractor` base class (and thus inheriting classes like `NightscoutExtractor`) automatically collects the following Prometheus metrics:

- `sling_extractor_duration_seconds` (Histogram): Duration of the `extract` method call. Labels: `extractor_name`.
- `sling_extractor_records_total` (Counter): Total number of records successfully returned by `sling.Sling().stream()`. Labels: `extractor_name`.
- `sling_extractor_errors_total` (Counter): Total number of times the `extract` method raised an exception. Labels: `extractor_name`.

**Note:** Full monitoring setup, including exposing these metrics via an HTTP endpoint and configuring Prometheus/Grafana, is typically handled as part of the overall system monitoring strategy (see Task 24). This instrumentation makes the metrics available for that system.

## Troubleshooting

- **Configuration Errors:** Check the YAML template syntax and ensure all required `config` keys for the specific extractor type are provided. Verify environment variables are set correctly if using `${VAR_NAME}` syntax.
- **Sling Errors:** Consult the `sling-python` documentation for errors related to specific sources or options within `sling_config`. Check API keys, connection details, and source paths.
- **Transformation Errors:** Look for warnings in the logs indicating skipped records due to data validation or parsing errors within the `extract` method of your specific extractor class.
- **Discovery Issues:** Ensure your custom extractor class is in the correct directory and inherits properly from `BaseExtractor` or `SlingExtractor`. Check for typos in the class name specified in the `type` field of the configuration template.
