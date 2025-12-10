# Test Scripts

This directory contains test scripts for the vast-admin-mcp tool.

## test_list_and_create.py

Comprehensive test script that validates:

1. **List Commands**: Tests various list commands including:
   - `views`
   - `tenants`
   - `viewpolicies`
   - `vippools`
   - `quotas`
   - `snapshots`

2. **Create View from Template**: Tests creating a view from the "vmware" template defined in `view_templates_example.json`

3. **Views Command with Arguments**: Tests the `views` list command:
   - Once without any arguments (returns all views)
   - Once with all available arguments using random values:
     - `cluster`: Random cluster from config
     - `tenant`: Random tenant
     - `name`: Random string filter (e.g., `*`, `*test*`, `test*`)
     - `path`: Random string filter
     - `bucket`: Random string filter
     - `share`: Random string filter
     - `logical_used`: Random size filter (e.g., `>20G`, `<4g`, `>=100MB`)
     - `physical_used`: Random size filter
     - `order`: Random field and direction (e.g., `name:asc`, `logical_used:desc`)
     - `top`: Random number (5-20)

### Usage

```bash
# Run the test script
python3 tests/test_list_and_create.py
```

### Prerequisites

- Valid `config.json` with at least one configured cluster
- Valid `view_templates_example.json` with a "vmware" template
- Network access to the configured VAST clusters
- Valid credentials for the clusters

### Output

The script will:
- Print test progress and results for each test
- Show success (✓) or failure (✗) for each operation
- Display a summary at the end

### Notes

- The script uses random values for testing, so results may vary between runs
- Some tests may fail if clusters are unreachable or templates don't exist
- The script is designed to be non-destructive (read-only operations except for view creation)

