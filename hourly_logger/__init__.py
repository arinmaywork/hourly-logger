"""Hourly Logger — modular core.

Public surface is intentionally small. Most consumers should import from the
specific submodule (config, database, sheets, …) rather than relying on
re-exports. The package version is exposed so logs can identify the build.
"""

__version__ = "2.0.0"
