# Quick Start Guide

## Prerequisites

1. **Rust** (1.70 or newer): Install from https://rustup.rs/
2. **Python** (3.8 or newer)
3. **pip** or **pip3**

## Installation Steps

### 1. Install Maturin

Maturin is the build tool for Rust-Python projects:

```bash
pip install maturin
# or
pip3 install maturin
# or
python3 -m pip install maturin --user
```

### 2. Install PyArrow

This is required for the library to work:

```bash
pip install pyarrow
```

### 3. Build and Install the Library

Navigate to the project directory and build in development mode:

```bash
cd /home/jassi/rust-projects/rdatacompy
maturin develop
```

This command will:
- Compile the Rust code
- Create Python bindings
- Install the package in your Python environment

For better performance, use release mode:

```bash
maturin develop --release
```

### 4. Test the Installation

Run the example script:

```bash
python examples/basic_usage.py
```

You should see detailed comparison output!

## Troubleshooting

### If maturin is not found

Make sure Python's bin directory is in your PATH:

```bash
# Linux/Mac
export PATH="$HOME/.local/bin:$PATH"

# Or use python -m
python3 -m maturin develop
```

### If Rust is not installed

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

### If you get Arrow/PyArrow errors

Make sure you have compatible versions:

```bash
pip install pyarrow>=14.0.0
```

## Development Workflow

1. **Make changes to Rust code** in `src/`
2. **Rebuild**: `maturin develop`
3. **Test**: Run your Python scripts or tests

## Running Tests

### Rust tests

```bash
cargo test
```

### Python tests (once pytest is set up)

```bash
pip install pytest pandas
pytest tests/
```

## Building Distribution Wheels

To create a wheel for distribution:

```bash
maturin build --release
```

The wheel will be in `target/wheels/`

## Next Steps

- See `architecture.md` for detailed design
- Check `examples/basic_usage.py` for usage examples
- Explore the API in `python/rdatacompy/__init__.pyi`
