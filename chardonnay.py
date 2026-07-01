"""Compatibility entrypoint for the ChardonnayKV package.

Prefer importing from `chardonnaykv` in new code:

    from chardonnaykv import ChardonnayKV
"""

from chardonnaykv import ChardonnayKV, demo, transfer

__all__ = ["ChardonnayKV", "demo", "transfer"]


if __name__ == "__main__":
    demo()
