"""Module entry point for python -m vast_admin_mcp."""

import sys

from .cli import main


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Program interrupted by user. Exiting...")
        sys.exit(0)
    except SystemExit:
        # Let SystemExit pass through (from sys.exit() calls)
        raise
    except Exception as e:
        # For known error types, show clean error message without traceback
        if isinstance(e, (ValueError, FileNotFoundError, PermissionError, KeyError)):
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        # For unknown errors, show full traceback for debugging
        import traceback
        traceback.print_exc()
        sys.exit(1)
