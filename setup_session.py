"""
Run this ONCE on your own machine to generate a Pyrogram session string.
This string is what lets the server upload files and stream from your Telegram account.

Usage:
    python setup_session.py

It will ask for:
    1. Your phone number
    2. The confirmation code Telegram sends you
    3. If you have 2FA enabled, your password

Then it prints a long string — copy that as SESSION_STRING on Render.
"""

from pyrogram import Client

import os

api_id = int(input("Enter your API_ID (from my.telegram.org): "))
api_hash = input("Enter your API_HASH (from my.telegram.org): ")

print("\nConnecting to Telegram...")
with Client(
    name="session_setup",
    api_id=api_id,
    api_hash=api_hash,
    in_memory=True,
) as client:
    client.start()
    session_string = client.export_session_string()
    print("\n" + "="*60)
    print("YOUR SESSION STRING (copy this to Render):")
    print("="*60)
    print(session_string)
    print("="*60)
    print("\nDone! You can close this now.")
