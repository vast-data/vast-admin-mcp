# YAML Template Structure Documentation

This document describes the structure of the YAML template file used to dynamically generate MCP tools for listing VAST cluster objects.

## File Location

The template system uses two files:
- **Default template**: `mcp_list_cmds_template.yaml` in the project root (shipped with the package)
  - Can be overridden via `VAST_ADMIN_MCP_DEFAULT_TEMPLATE_FILE` environment variable
- **Template modifications**: `~/.vast-admin-mcp/mcp_list_template_modifications.yaml` (user customizations)
  - Can be overridden via `VAST_ADMIN_MCP_TEMPLATE_MODIFICATIONS_FILE` environment variable

The template modifications file is merged with the default template, allowing users to override specific fields and properties without modifying the default file.

## Overview

The YAML file uses a structured format with five main sections:
- **api_whitelist**: API endpoint whitelist for security (restrictive by default)
- **variables**: String replacements for use in descriptions
- **field_anchors**: Reusable field definitions using YAML anchors
- **list_cmds**: Command definitions for dynamic list tools
- **merged_list_cmds**: Merged command definitions that combine multiple list commands

## API Whitelist

The `api_whitelist` section controls which VAST API endpoints and HTTP methods can be accessed. This provides security by restricting API access.

### Default Behavior

- **Simple format** (`- views`): Defaults to **GET only**
- **With methods** (`- views: [post]`): Allows **GET + specified methods**
  - Example: `- views: [post]` enables both GET and POST for the views endpoint
  - Example: `- quotas: [post, patch]` enables GET, POST, and PATCH for the quotas endpoint

### Format

```yaml
api_whitelist:
  # Simple format - GET only
  - clusters
  - tenants
  
  # With methods - GET + specified methods
  - views: [post]  # GET + POST for create operations
  - snapshots: [post]  # GET + POST for create operations
  - quotas: [post, patch]  # GET + POST + PATCH for create/update operations
```

### Security Model

- **Restrictive by default**: If an endpoint is not in the whitelist, it is denied
- **Method validation**: Only specified HTTP methods are allowed
- **Sub-endpoint support**: If a parent endpoint is whitelisted (e.g., `monitors`), all sub-endpoints are allowed (e.g., `monitors.ad_hoc_query`)
- **All API calls validated**: Both list and create functions use the whitelist for validation

### Why This Matters

All API calls (including create functions) are validated against the whitelist. This ensures:
- Only approved endpoints can be accessed
- Only approved HTTP methods can be used
- Create operations require explicit whitelist configuration (e.g., `- views: [post]`)

## YAML File Structure

The YAML file has the following top-level structure:

```yaml
# API Whitelist - Restrictive by default (deny all if empty or missing)
# Simple format defaults to GET only
# With methods adds to GET (e.g., [post] enables GET + POST)
api_whitelist:
  - clusters  # GET only
  - views: [post]  # GET + POST
  - tenants  # GET only
  # ... more endpoints

# Variables for string replacement in descriptions
variables:
  cluster_warning: |-
    ⚠️ IMPORTANT: ...

# Reusable field definitions using YAML anchors
field_anchors:
  cluster_field: &cluster_field
    name: cluster
    argument: ...
  tenant_field: &tenant_field
    name: tenant
    field: tenant_name
    argument: ...

# List command definitions
list_cmds:
  views:
    api_endpoints: [...]
    fields: [...]
    description: |-
      ...
  volumes:
    ...
```

### Variables Section

The `variables` section (optional) defines string replacements that can be used in descriptions using `{{variable_name}}` syntax.

**Type**: Dictionary of string keys to string values

**Example**:
```yaml
variables:
  cluster_warning: |-
    ⚠️ IMPORTANT: By default, when no cluster is specified...
```

**Usage in descriptions**:
```yaml
description: |-
  Use this tool...
  {{cluster_warning}}
  ...
```

Variables are automatically replaced during template loading. Special placeholders `{{$arguments}}` and `{{$fields}}` are NOT replaced by variables (they are handled separately).

### Field Anchors Section

The `field_anchors` section (optional) defines reusable field configurations using YAML anchors. These can be referenced in `list_cmds` using the anchor syntax `<<: *anchor_name`.

**Type**: Dictionary of anchor names to field definitions

**Example**:
```yaml
field_anchors:
  cluster_field: &cluster_field
    name: cluster
    argument:
      type: list
      mandatory: false
      ...
```

**Usage in list_cmds**:
```yaml
list_cmds:
  views:
    fields:
      - <<: *cluster_field
      - name: tenant
        ...
```

### List Commands Section

The `list_cmds` section (required) contains all command definitions. Each command represents a dynamic list tool that will be registered as an MCP tool.

**Type**: Dictionary of command names to command definitions

**Optional Command-Level Properties**:

- **`create_mcp_tool`** (boolean, default: `true`): If set to `false`, the command will not be registered as a standalone MCP tool. The command can still be used in merged commands and via CLI, but won't appear as a separate MCP tool. This is useful for commands that should only be available as part of merged commands.

**Example**:
```yaml
list_cmds:
  cnodes:
    create_mcp_tool: false  # Hide from MCP server, only available in merged commands
    api_endpoints:
      - cnodes
      - cboxes
    # ... rest of command definition
```

## Command Structure

Each command in `list_cmds` must contain the following sections:

### 1. `api_endpoints` (Required)

Defines which VAST API endpoints to call. The first endpoint is the primary data source, and additional endpoints are joined with it based on a common key.

**Type**: List of api endpoints. 

**Example**:
```yaml
api_endpoints:
  - views
  - quotas
```

**Supported Endpoints**: `views`, `quotas`, `tenants`, `vippools`, `snapshots`, `volumes`, etc. The full list of api endpoints can be seen in the VAST API documentation.

### 2. `fields` (Required)

Defines the output fields with their transformations and display options.

**Type**: List of field definitions

**Field Definition Structure**:
```yaml
# For output fields (display only)
- name: <display_name>          # Required: Field name
  field: <source_field>         # Optional: Source field name (defaults to name). When secondary API provided use: secondary_api.field
  value: <f-string_expression>  # Optional: F-string expression to compute field value
  limit_table_column_width: <int> # Optional: Truncate display width
  jq: <jq_expression>          # Optional: jq transformation
  convert: <unit>               # Optional: Unit conversion (KB|MB|GB|TB|PB|AUTO|time_delta)
  join_on:                      # Optional: Join multiple API results 
    field: base_field           # Required: Field on primary APi
    on_field: "joined_field"    # Required: Field on secondary API
    act_on: "first|last|all"    # Optional: joins (act_on defaults to "first")
  condition:                    # Optional: Condition to evaluate before including field
    field: <field_name>         # Required: Field name to check in row data
    operator: <operator>        # Required: Comparison operator (see supported operators below)
    value: <value>              # Required: Value to compare against
  hide: <bool>                  # Optional: Hide field in output (default: false)

# For fields that are also CLI/MCP arguments
- name: <argument_name>         # Required: Argument name (also used as field name)
  field: <api_field_name>       # Optional: API parameter name (defaults to name)
  argument:                     # Required: Argument configuration
    type: <str|int|bool|list|capacity>  # Optional: Argument type (default: str)
    mandatory: <bool>           # Optional: Required argument (default: false)
    description: <string>       # Optional: Argument description
    regex_validation: <pattern> # Optional: Validation regex pattern
    filter: <bool>              # Optional: Enable wildcard filtering (default: false)
    client_side_filter: <bool>  # Optional: Force client-side only filtering (default: false). If true, skips API filtering and only applies client-side filtering after data retrieval.
    argument_list: <bool>       # Optional: For list type, treat as comma-separated (default: false)
    aliases: [<alias1>, ...]    # Optional: List of alternative argument names usefull for MCP context
```

**Field Source Types**:

- **Direct field**: `field: path` - Uses `row['path']` from API response
- **Renamed field**: `field: tenant_name` - Uses `row['tenant_name']` but displays as `name`
- **CLI parameter**: `field: $(cluster)` - Uses CLI argument value (e.g., cluster name)
- **Joined field**: `field: quotas.hard_limit` - Uses joined data from secondary API endpoint
- **Per-row endpoint field**: `field: capacity.root_data` - Uses data from per-row API endpoint

**Transformations**:

- **value**: Set field value using Python f-string expression
  - **Syntax**: Must be a valid Python f-string (starts with `f"` or `f'`)
  - **Field References**: Reference other fields from the same row using `{field_name}`
  - **Helper Functions**: Available string manipulation functions:
    - `lower(s)`: Convert string to lowercase
    - `upper(s)`: Convert string to uppercase
    - `concat(*args)`: Concatenate multiple strings
    - `replace(s, old, new)`: Replace occurrences of `old` with `new` in string
    - `substring(s, start, end)`: Extract substring (end is optional)
    - `strip(s)`: Remove leading and trailing whitespace
    - `join(separator, *args)`: Join strings with separator
  - **Examples**:
    ```yaml
    - name: display_name
      value: 'f"async/{lower(role)}"'
    - name: full_path
      value: 'f"{tenant}/{path}"'
    - name: status_label
      value: 'f"Status: {upper(status)}"'
    - name: combined
      value: 'f"{concat(prefix, \'_\', suffix)}"'
    - name: normalized
      value: 'f"{strip(lower(name))}"'
    ```
  - **Note**: When `value` is specified, the field value is computed from the expression rather than read from a source field. The expression is evaluated BEFORE other transformations (jq, convert, etc.)

- **jq**: Apply jq transformations using the system `jq` command
  - **Requirement**: The `jq` command-line tool must be installed on the system
  - **Examples**:
    - `join(",")`: Join list elements with comma
    - `join("|")`: Join list elements with pipe
    - `join(" ")`: Join list elements with space
    - `.[0]`: Get first element of array
    - `.[1]`: Get second element of array
    - `length`: Get length of array or string
    - Any valid jq expression can be used

- **convert**: Convert bytes to human-readable units or timestamps
  - **Capacity units**: `KB`, `MB`, `GB`, `TB`, `PB`, `AUTO`
  - **Time units**: `time_delta` (converts ISO timestamp to "Xd Xh Xm Xs ago" or "in Xd Xh Xm Xs")
  - `AUTO` selects the best-fit unit automatically for capacity

- **limit_table_column_width**: Truncate long strings for display
  - Example: `limit_table_column_width: 20` truncates to 17 chars + "..."

- **hide**: Exclude field from output (useful for intermediate calculations)

- **condition**: Conditionally include field based on row data evaluation
  - If condition evaluates to `False`, the field is skipped (not included in output)
  - If condition evaluates to `True`, the field is processed normally
  - **Supported Operators**:
    - **String operators**: `equals` (or `==`, `eq`), `not_equals` (or `!=`, `ne`), `contains`, `starts_with`, `ends_with`, `in`, `regex`
    - **Numeric operators**: `equals`, `not_equals`, `greater_than` (or `>`, `gt`), `less_than` (or `<`, `lt`), `greater_equal` (or `>=`, `gte`), `less_equal` (or `<=`, `lte`), `in`
    - **Boolean operators**: `equals`, `not_equals`
    - **Time operators**: `equals`, `not_equals`, `greater_than`, `less_than`, `greater_equal`, `less_equal`
  - **Type Detection**: Field type is automatically detected from the row value (str, int, bool, datetime)
  - **Value Conversion**: Condition value is automatically converted to match the field type when possible
  - **Example**:
    ```yaml
    - name: box name
      field: cboxes.name
      join_on:
        field: box_id
        on_field: id
      condition:
        field: box_type
        operator: equals  # or ==
        value: "cbox"
    ```

**Argument Fields**:

Fields with an `argument` property become CLI/MCP arguments that users can provide:
- The field's `name` becomes the argument name
- The field's `field` property (if present) maps to the API parameter name
- If `field` is not specified, the argument name is used as the API parameter name
- Special fields like `cluster` that don't map to API parameters should not have a `field` property

**Filtering Behavior**:

- **`filter: true`**: Enables filtering on this field. By default, filtering is attempted on both server-side (API) and client-side (validation/backup).
- **`client_side_filter: true`**: Forces client-side only filtering. When set, the filter is NOT sent to the API and is only applied after data retrieval. This is useful for:
  - Computed fields (fields with `value:` property) that don't exist in the API response
  - Fields where API filtering doesn't work correctly
  - Ensuring consistent filtering behavior regardless of API capabilities

**Example**:
```yaml
- name: protection type
  value: 'f"local"'
  condition:
    field: role
    operator: "=="
    value: "Local"
  argument:
    type: str
    mandatory: false
    filter: true
    client_side_filter: true  # Force client-side only (computed field)
```

**Join Configuration**:

For fields that join data from multiple API endpoints, use the `join_on` property:

```yaml
# Simple join (same field name in both endpoints)
- name: hard quota
  field: quotas.hard_limit
  join_on: path

# Advanced join (different field names)
- name: hard quota
  field: quotas.hard_limit
  join_on:
    field: path          # Field from base endpoint
    on_field: view_path  # Field from joined endpoint

# Join with act_on to control behavior when multiple matches exist
- name: views
  field: views.path
  join_on:
    field: name          # Field from base endpoint
    on_field: policy    # Field from joined endpoint
    act_on: all         # Options: first (default), last, all
```

**`act_on` Behavior**:
- **`first`** (default): When multiple items match the join key, use the first matching item
- **`last`**: When multiple items match the join key, use the last matching item
- **`all`**: When multiple items match the join key, aggregate all matching items. For field access, values are joined with `"\n"` (newline). For example, if a policy has multiple matching views, `views.path` will return `"/view1\n/view2\n/view3"`

### 3. `per_row_endpoints` (Optional)

Defines endpoints that should be called for each row in the base dataset. Useful when data needs to be fetched individually per row.

**Type**: List of endpoint configurations

**Example**:
```yaml
per_row_endpoints:
  - name: capacity
    query:
      - "tenant_id=$id"    # Use 'id' field from row
      - "path=/"           # Literal value
```

**Query Parameter Syntax**:
- `key=$field_name`: References a field from the current row (e.g., `$id` uses `row['id']`)
- `key=value`: Literal value (e.g., `path=/`)

**Accessing Per-Row Data**:
Per-row endpoint data is accessed using `endpoint.field` syntax:
```yaml
- name: usable capacity
  field: capacity.root_data  # Access 'root_data' from 'capacity' endpoint
  jq: .[0]
  convert: AUTO
```

### 4. `description` (Required)

Command description used for MCP context. Supports special placeholders:
- `{{$arguments}}`: Automatically replaced with formatted argument list
- `{{$fields}}`: Automatically replaced with formatted field list
- `{{variable_name}}`: Replaced with value from `variables` section

**Type**: String (multi-line supported with `|-`)

**Example**:
```yaml
description: |-
  Use this tool to retrieve a list of views from VAST cluster(s).
  {{cluster_warning}}
  Args:
    {{$arguments}}
  Returns:
    A list of views...
  Fields:
    {{$fields}}
```

## Validation

The YAML file is validated when loaded. Validation errors include:
- **Field paths**: Errors show the full path to the problematic field (e.g., `list_cmds.views.fields[2].argument.type`)
- **Error types**: `InvalidType`, `MissingRequired`, `InvalidValue`
- **Context**: Shows expected vs actual values

**Example Error Messages**:
```
list_cmds.views.fields[2].argument.type: InvalidValue - Expected one of ['str', 'int', 'bool', 'list', 'capacity'], got 'bool'
list_cmds.volumes.api_endpoints: InvalidType - Expected list, got dict
list_cmds.tenants.fields[0]: MissingRequired - Field must have 'header' or 'name' key
```

## Complete Example

```yaml
# Variables section
variables:
  cluster_warning: |-
    ⚠️ IMPORTANT: By default, when no cluster is specified...

# Field anchors section
field_anchors:
  cluster_field: &cluster_field
    name: cluster
    argument:
      type: list
      mandatory: false
      description: >-
        Comma-separated list of cluster names. {{cluster_warning}}
      argument_list: true

# List commands section
list_cmds:
  views:
    api_endpoints:
      - views
      - quotas
    
    fields:
      # Use field anchor
      - <<: *cluster_field
      
      # Regular field with argument
      - name: tenant
        field: tenant_name
        argument:
          type: str
          mandatory: false
          filter: true
      
      # Field with transformation
      - name: protocols
        jq: join(",")
      
      # Field with unit conversion
      - name: logical used
        field: logical_capacity
        convert: AUTO
        argument:
          type: int
          filter: true
      
      # Joined field
      - name: hard quota
        field: quotas.hard_limit
        join_on:
          field: path
          on_field: path
        convert: AUTO
      
      # Conditional field (only included when condition is true)
      - name: box name
        field: cboxes.name
        join_on:
          field: box_id
          on_field: id
        condition:
          field: box_type
          operator: equals
          value: "cbox"
      
      # Conditional field (only included when condition is true)
      - name: box name
        field: cboxes.name
        join_on:
          field: box_id
          on_field: id
        condition:
          field: box_type
          operator: equals
          value: "cbox"
    
    description: |-
      Use this tool to retrieve a list of views from VAST cluster(s).
      {{cluster_warning}}
      Args:
        {{$arguments}}
      Returns:
        A list of views...
      Fields:
        {{$fields}}
```

## Merged List Commands

Merged list commands allow you to combine multiple list commands into a single unified MCP function. This is useful when you want to query related resources together (e.g., combining `cnodes` and `dnodes` into a `topology` command).

### Structure

Merged commands are defined in the `merged_list_cmds` section of the YAML file:

```yaml
merged_list_cmds:
  - name: topology
    functions:
      - cnodes
      - dnodes
    description: |-
      Use this tool to retrieve a list of cnodes and dnodes from VAST cluster(s).
      Args:
        {{$arguments}}
      Returns:
        A list of cnodes and dnodes. Each item in the list will be a dictionary containing general information regarding a specific cnode or dnode.
      Fields:
        {{$fields}}
```

### Required Properties

- **`name`** (string, required): The name of the merged command. This will be used to create the MCP tool `list_{name}_vast`.
- **`functions`** (list, required): List of source function names to merge. Must contain at least 2 function names, and all function names must exist in the `list_cmds` section.
- **`description`** (string, required): Description of the merged command. Must contain both `{{$arguments}}` and `{{$fields}}` placeholders.

### How Merging Works

1. **Arguments**: Arguments from all source functions are merged using a union approach:
   - All unique arguments from all source functions are included
   - If the same argument name exists in multiple functions, the first occurrence's configuration is used (type, description, aliases, etc.)
   - Argument order: first function's arguments first, then unique arguments from others

2. **Fields**: Fields are merged in order:
   - All fields from the first function (in original order)
   - Unique fields from the second function (not in first)
   - Unique fields from the third function (not in first or second)
   - Continue for all functions

3. **Results**: Results from all source functions are concatenated:
   - Each source function is executed with the same arguments
   - Results are combined into a single list
   - Missing fields in rows from one function are set to `None` to ensure consistent structure
   - All rows have the same field structure (merged field set)

### Example

Given two source functions:

**cnodes** returns:
- `name` (string)
- `box_type` (string)
- `box_name` (string, conditional)

**dnodes** returns:
- `name` (string)
- `box_type` (string)
- `rack_name` (string)

The merged `topology` command will:
- Accept all unique arguments from both `cnodes` and `dnodes`
- Return rows with fields: `name`, `box_type`, `box_name`, `rack_name`
- Rows from `cnodes` will have `rack_name = None`
- Rows from `dnodes` will have `box_name = None`

### Validation

The following validations are performed:
- `name` must be a non-empty string
- `functions` must be a list with at least 2 items
- All function names in `functions` must exist in `list_cmds`
- `description` must be a non-empty string
- `description` must contain `{{$arguments}}` placeholder
- `description` must contain `{{$fields}}` placeholder

## Template File Merging

The system merges a default template file (in the project root) with a user template file (`~/.vast-admin-mcp/mcp_list_cmds_template.yaml`). This allows users to customize specific settings without modifying the default template.

### Merge Strategy

- **api_whitelist**: Union merge - combines both lists (user entries take precedence for duplicates)
- **variables**: Dictionary merge - user variables override/add to default
- **field_anchors**: Dictionary merge - user anchors override/add to default
- **list_cmds**: Deep merge - user can override:
  - Entire commands (user command replaces default)
  - Command-level properties (e.g., `create_mcp_tool: false`)
  - Specific fields within commands (e.g., add `hide: true` to a field)
  - Field properties (e.g., `argument.filter`, `argument.client_side_filter`)
- **merged_list_cmds**: Deep merge (same strategy as list_cmds)

### Field Matching

When merging field lists in commands, fields are matched by their `name` property. This allows users to:
- Override specific field properties (e.g., add `hide: true` to an existing field)
- Override nested properties (e.g., `argument.filter: true`)
- Add new fields
- Keep default fields that aren't overridden

### Example User File

Users can create a minimal override file to customize specific settings:

```yaml
list_cmds:
  cnodes:
    create_mcp_tool: false  # Disable MCP tool for this command
  views:
    fields:
      - name: qos policy
        hide: true  # Hide this specific field
      - name: protocols
        hide: true  # Hide another field
      - name: tenant
        argument:
          filter: false  # Override filter setting for tenant field
```

### Missing Files

- If default template is missing: Uses user template only (if it exists)
- If user template is missing: Uses default template only
- If both are missing: Raises an error

## Notes

- YAML anchors defined in `field_anchors` are automatically available in `list_cmds` (YAML handles this)
- Variable replacements are applied recursively to all string values
- Special placeholders `{{$arguments}}` and `{{$fields}}` are replaced after variable replacements
- All validation errors include full field paths for easier debugging
- User template merges with default template before validation and processing
