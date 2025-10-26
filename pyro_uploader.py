import os
from typing import Optional
from pyrogram import Client

_pyro = None

def get_pyro() -> Optional[Client]:
    global _pyro
    if _pyro is not None:
        return _pyro
    api_id = os.environ.get('PYROGRAM_API_ID')
    api_hash = os.environ.get('PYROGRAM_API_HASH')
    session = os.environ.get('PYROGRAM_SESSION_STRING')
    if not (api_id and api_hash and session):
        return None
    _pyro = Client(name=':memory:', api_id=int(api_id), api_hash=api_hash, session_string=session, workdir='/tmp')
    return _pyro
