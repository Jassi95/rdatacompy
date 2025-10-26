# Publishing RDataCompy to PyPI

## Prerequisites Checklist

Before publishing to PyPI, ensure you have:

- [x] LICENSE file (Apache-2.0)
- [x] README.md with usage examples
- [x] pyproject.toml configured
- [ ] Update author information in pyproject.toml
- [ ] Create PyPI account at https://pypi.org/account/register/
- [ ] Create TestPyPI account at https://test.pypi.org/account/register/
- [ ] Install publishing tools

## Step-by-Step Publishing Guide

### 1. Install Required Tools

```bash
pip install maturin twine
```

### 2. Update Project Metadata

Edit `pyproject.toml` and update:
- `authors` - Replace "Your Name" and email
- Add project URLs (homepage, repository, documentation)

```toml
[project]
# ... existing fields ...
authors = [
    { name = "Your Name", email = "your.email@example.com" }
]

[project.urls]
Homepage = "https://github.com/yourusername/rdatacompy"
Repository = "https://github.com/yourusername/rdatacompy"
Issues = "https://github.com/yourusername/rdatacompy/issues"
```

### 3. Test Build Locally

```bash
# Build wheels for your current platform
maturin build --release

# This creates wheels in target/wheels/
# Example: rdatacompy-0.1.0-cp312-cp312-linux_x86_64.whl
```

### 4. Test Installation Locally

```bash
# Install the built wheel
pip install target/wheels/rdatacompy-*.whl

# Test it works
python -c "from rdatacompy import Compare; print('✓ Import successful')"
```

### 5. Publish to TestPyPI (Recommended First!)

```bash
# Build the package
maturin build --release

# Upload to TestPyPI
maturin upload --repository testpypi target/wheels/*

# You'll be prompted for TestPyPI credentials
# Username: __token__
# Password: pypi-... (your TestPyPI API token)
```

### 6. Test Install from TestPyPI

```bash
# Create a fresh virtual environment
python -m venv test_env
source test_env/bin/activate

# Install from TestPyPI
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ rdatacompy

# Test it
python -c "from rdatacompy import Compare; print('✓ TestPyPI installation successful')"
```

### 7. Publish to Real PyPI

Once TestPyPI works:

```bash
# Build fresh wheels
maturin build --release

# Upload to PyPI
maturin upload target/wheels/*

# You'll be prompted for PyPI credentials
# Username: __token__
# Password: pypi-... (your PyPI API token)
```

### 8. Verify Installation

```bash
# Install from real PyPI
pip install rdatacompy

# Test
python -c "from rdatacompy import Compare; print('✓ PyPI installation successful')"
```

## Using API Tokens (Recommended)

### Create PyPI API Token:
1. Go to https://pypi.org/manage/account/token/
2. Click "Add API token"
3. Name: "rdatacompy-upload"
4. Scope: "Entire account" or specific project
5. Copy the token (starts with `pypi-...`)

### Create TestPyPI API Token:
1. Go to https://test.pypi.org/manage/account/token/
2. Same process as above

### Configure with maturin:

```bash
# Option 1: Environment variables
export MATURIN_PYPI_TOKEN="pypi-your-token-here"
maturin upload target/wheels/*

# Option 2: Pass as argument
maturin upload --token pypi-your-token-here target/wheels/*
```

## Automated Publishing with GitHub Actions

Create `.github/workflows/publish.yml`:

```yaml
name: Publish to PyPI

on:
  release:
    types: [published]

jobs:
  linux:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: PyO3/maturin-action@v1
        with:
          manylinux: auto
          command: build
          args: --release --sdist -o dist
      - name: Upload wheels
        uses: PyO3/maturin-action@v1
        with:
          command: upload
          args: --skip-existing dist/*
        env:
          MATURIN_PYPI_TOKEN: ${{ secrets.PYPI_API_TOKEN }}

  macos:
    runs-on: macos-latest
    steps:
      - uses: actions/checkout@v4
      - uses: PyO3/maturin-action@v1
        with:
          command: build
          args: --release -o dist --universal2
      - name: Upload wheels
        uses: PyO3/maturin-action@v1
        with:
          command: upload
          args: --skip-existing dist/*
        env:
          MATURIN_PYPI_TOKEN: ${{ secrets.PYPI_API_TOKEN }}

  windows:
    runs-on: windows-latest
    steps:
      - uses: actions/checkout@v4
      - uses: PyO3/maturin-action@v1
        with:
          command: build
          args: --release -o dist
      - name: Upload wheels
        uses: PyO3/maturin-action@v1
        with:
          command: upload
          args: --skip-existing dist/*
        env:
          MATURIN_PYPI_TOKEN: ${{ secrets.PYPI_API_TOKEN }}
```

Then add `PYPI_API_TOKEN` as a GitHub secret.

## Version Bumping

Before each release:

1. Update version in `pyproject.toml` and `Cargo.toml`
2. Update `python/rdatacompy/__init__.py` (`__version__`)
3. Create git tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```

## Common Issues & Solutions

### Issue: "File already exists"
**Solution**: Increment version number, you can't overwrite published versions

### Issue: "Invalid wheel filename"
**Solution**: Ensure maturin is up to date: `pip install -U maturin`

### Issue: Platform-specific wheels
**Solution**: Use GitHub Actions to build for Linux, macOS, Windows automatically

### Issue: "Import error after install"
**Solution**: Check module name in `pyproject.toml` matches: `module-name = "rdatacompy._rdatacompy"`

## Quick Publish Checklist

- [ ] Update version in `pyproject.toml`, `Cargo.toml`, `__init__.py`
- [ ] Update README.md and CHANGELOG.md
- [ ] Update author info in `pyproject.toml`
- [ ] `maturin build --release`
- [ ] Test wheel locally
- [ ] `maturin upload --repository testpypi target/wheels/*`
- [ ] Test install from TestPyPI
- [ ] `maturin upload target/wheels/*` (real PyPI)
- [ ] Create git tag
- [ ] Update documentation

## Resources

- PyPI: https://pypi.org
- TestPyPI: https://test.pypi.org
- Maturin docs: https://www.maturin.rs/
- PyPI packaging guide: https://packaging.python.org/
