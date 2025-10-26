#!/bin/bash

# Build script for rdatacompy
# This script builds the Rust library and shows you how to install it

echo "=================================================="
echo "Building rdatacompy..."
echo "=================================================="

# Option 1: If you have python3-venv installed
echo ""
echo "Option 1: Using virtual environment (recommended)"
echo "--------------------------------------------------"
echo "Run these commands:"
echo ""
echo "  # Install python3-venv if needed:"
echo "  sudo apt install python3-venv"
echo ""
echo "  # Create virtual environment:"
echo "  python3 -m venv .venv"
echo ""
echo "  # Activate it:"
echo "  source .venv/bin/activate"
echo ""
echo "  # Install dependencies:"
echo "  pip install maturin pyarrow"
echo ""
echo "  # Build and install:"
echo "  maturin develop --release"
echo ""

# Option 2: Build wheel and install manually
echo ""
echo "Option 2: Build wheel manually (no venv needed)"
echo "------------------------------------------------"
echo "Run these commands:"
echo ""
echo "  # Build the wheel:"
echo "  maturin build --release"
echo ""
echo "  # Install the wheel:"
echo "  pip3 install --user target/wheels/rdatacompy-*.whl --force-reinstall"
echo ""
echo "  # Or with sudo:"
echo "  sudo pip3 install target/wheels/rdatacompy-*.whl --force-reinstall"
echo ""

echo "=================================================="
echo "Choose one option above and run the commands"
echo "=================================================="
