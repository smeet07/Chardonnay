"""Run the ChardonnayKV transfer demo."""

from pathlib import Path
import sys


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from chardonnaykv import demo


if __name__ == "__main__":
    demo()
