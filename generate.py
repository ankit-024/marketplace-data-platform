"""
Root Entrypoint for Marketplace Data Generator
Usage:
    python generate.py --customers 5000 --sellers 500 --orders 20000 --skew true --seed 42
"""

from data_generator.main import main

if __name__ == "__main__":
    main()
