# Release Process

This document describes the release process for VAST Admin MCP Server.

## Version Management

The project uses semantic versioning (MAJOR.MINOR.PATCH):
- **MAJOR**: Incompatible API changes
- **MINOR**: New functionality (backwards compatible)
- **PATCH**: Bug fixes (backwards compatible)

Version is stored in: `src/vast_admin_mcp/__about__.py`

## Release Checklist

### 1. Update Version

Use the version bump script:

```bash
# Bump patch version (0.1.0 -> 0.1.1)
./bump-version.sh patch

# Bump minor version (0.1.0 -> 0.2.0)
./bump-version.sh minor

# Bump major version (0.1.0 -> 1.0.0)
./bump-version.sh major

# Set specific version
./bump-version.sh 1.5.2
```

### 2. Update CHANGELOG (if exists)

Document changes in the new version:
- New features
- Bug fixes
- Breaking changes
- Deprecations

### 3. Commit and Tag

```bash
# Review changes
git diff src/vast_admin_mcp/__about__.py

# Commit version bump
git add src/vast_admin_mcp/__about__.py
git commit -m "Bump version to X.Y.Z"

# Create annotated tag
git tag -a vX.Y.Z -m "Release vX.Y.Z"

# Push commits and tags
git push origin main
git push origin vX.Y.Z
```

### 4. Build Python Package

```bash
# Install build tools (if not already installed)
pip install --upgrade build twine

# Build distribution packages
python -m build

# This creates:
# - dist/vast_admin_mcp-X.Y.Z-py3-none-any.whl
# - dist/vast_admin_mcp-X.Y.Z.tar.gz
```

### 5. Build Docker Image

```bash
# Build with version tags
make build-docker

# This creates:
# - vast-admin-mcp:X.Y.Z
# - vast-admin-mcp:latest
```

### 6. Test Release

```bash
# Test pip package in virtual environment
python -m venv test-env
source test-env/bin/activate
pip install dist/vast_admin_mcp-X.Y.Z-py3-none-any.whl
vast-admin-mcp --version
deactivate
rm -rf test-env

# Test Docker image
docker run --rm vast-admin-mcp:X.Y.Z vast-admin-mcp --version
```

### 7. Publish to PyPI

```bash
# Upload to PyPI (requires PyPI credentials)
python -m twine upload dist/vast_admin_mcp-X.Y.Z*

# Or upload to Test PyPI first
python -m twine upload --repository testpypi dist/vast_admin_mcp-X.Y.Z*
```

### 8. Push Docker Image (if using registry)

```bash
# Tag for your registry
docker tag vast-admin-mcp:X.Y.Z your-registry/vast-admin-mcp:X.Y.Z
docker tag vast-admin-mcp:latest your-registry/vast-admin-mcp:latest

# Push to registry
docker push your-registry/vast-admin-mcp:X.Y.Z
docker push your-registry/vast-admin-mcp:latest
```

### 9. Create GitHub Release

1. Go to: https://github.com/vast-data/vast-admin-mcp/releases/new
2. Select the tag: `vX.Y.Z`
3. Release title: `vX.Y.Z`
4. Description: Copy from CHANGELOG
5. Attach files:
   - `dist/vast_admin_mcp-X.Y.Z-py3-none-any.whl`
   - `dist/vast_admin_mcp-X.Y.Z.tar.gz`
6. Publish release

## Version Information in Artifacts

### Python Package
- Version in: `src/vast_admin_mcp/__about__.py`
- Accessible via: `import vast_admin_mcp; print(vast_admin_mcp.__version__)`

### Docker Image
- Version in: Build arg `VERSION`
- Labels: `org.opencontainers.image.version`
- Environment: `VAST_ADMIN_MCP_VERSION`

To inspect Docker image version:
```bash
# Check version label
docker inspect vast-admin-mcp:latest | jq '.[0].Config.Labels["org.opencontainers.image.version"]'

# Check all labels
docker inspect vast-admin-mcp:latest | jq '.[0].Config.Labels'

# Check environment variable
docker run --rm vast-admin-mcp:latest env | grep VAST_ADMIN_MCP_VERSION
```

## Rollback

If issues are found after release:

1. **Yank from PyPI** (doesn't delete, just marks as unavailable):
   ```bash
   # Contact PyPI support or use web interface
   ```

2. **Revert Git tag**:
   ```bash
   git tag -d vX.Y.Z
   git push origin :refs/tags/vX.Y.Z
   ```

3. **Release hotfix**:
   ```bash
   ./bump-version.sh patch
   # Fix issues, then follow release process
   ```

## Automation (Future)

Consider setting up GitHub Actions for:
- Automated testing on tag push
- Automated PyPI publishing
- Automated Docker image building and pushing
- Automated GitHub release creation

