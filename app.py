import asyncio
import logging
import os
import uuid
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
    CallbackQueryHandler,
)

from auth_db import set_authorised, get_authorisation, is_authorised, add_admin, remove_admin, is_admin, list_admins
from s3_io import presign_get, multipart_uploader
from ffmpeg_stream import (
    audio_audio_to_stdout,
    video_video_to_stdout,
    video_subtitle_to_stdout,
    video_audio_to_stdout,
)
from gplinks import shorten_with_gplinks
from pyro_uploader import get_pyro

import httpx

logging.basicConfig(
    format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
    level=logging.INFO,
)
log = logging.getLogger('merge-bot')

BOT_TOKEN = os.environ.get('BOT_TOKEN')
ALLOWED_CHAT = os.environ.get('ALLOWED_CHAT')
S3_BUCKET = os.environ.get('S3_BUCKET')
MASTER_GPLINKS_API = os.environ.get('MASTER_GPLINKS_API')
ADMINS_ENV = os.environ.get('ADMINS','')

MENU = (
    ('🎬 Video + Video', 'op_vv'),
    ('🎵 Audio + Audio', 'op_aa'),
    ('🎞️ Video + Subtitle', 'op_vs'),
    ('🎧 Video + Audio', 'op_va'),
)

@dataclass
class Session:
    op: Optional[str] = None
    files: List[Tuple[str, str]] = field(default_factory=list)  # (tg_file_id, s3_key)
    captions: List[str] = field(default_factory=list)

_sessions = {}

def _get_session(user_id: int) -> Session:
    s = _sessions.get(user_id)
    if not s:
        s = Session()
        _sessions[user_id] = s
    return s

def _kb():
    return InlineKeyboardMarkup([[InlineKeyboardButton(text, callback_data=data)] for text, data in MENU])

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text('Hi! Choose an operation:', reply_markup=_kb())

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text(
        'Supported merges: Video+Video, Audio+Audio, Video+Subtitle, Video+Audio.\n'
        'Use /authorise <telegram_id> <gplinks_token> to enable 12h downloads.'
    )

async def authorise_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    args = (update.message.text or '').split(maxsplit=2)
    if len(args) < 2:
        await update.effective_message.reply_text('Usage: /authorise <telegram_id> [gplinks_token]')
        return
    caller = update.effective_user.id
    try:
        target_id = int(args[1])
    except ValueError:
        await update.effective_message.reply_text('telegram_id must be an integer.')
        return
    provided = args[2].strip() if len(args) >= 3 else ''
    token = provided
    if not token:
        if await is_admin(caller) and MASTER_GPLINKS_API:
            token = MASTER_GPLINKS_API
        else:
            await update.effective_message.reply_text('Missing gplinks_token (only admins may omit it).')
            return
    expires = await set_authorised(target_id, token, 12)
    await update.effective_message.reply_text(f'Authorised {target_id} for 12h (until {expires.isoformat()}).')

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    u = update.effective_user
    doc = await get_authorisation(u.id)
    if not doc:
        await update.effective_message.reply_text('Not authorised.')
    else:
        await update.effective_message.reply_text(f"Authorised until {doc.get('expires_at')}")



async def addadmin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    caller = update.effective_user.id
    if not await is_admin(caller):
        await update.effective_message.reply_text("Admins only.")
        return
    args = (update.message.text or "").split(maxsplit=1)
    if len(args) < 2:
        await update.effective_message.reply_text("Usage: /addadmin <telegram_id>")
        return
    try:
        target = int(args[1])
    except ValueError:
        await update.effective_message.reply_text("telegram_id must be an integer.")
        return
    await add_admin(target)
    await update.effective_message.reply_text(f"Added admin: {target}")

async def deladmin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    caller = update.effective_user.id
    if not await is_admin(caller):
        await update.effective_message.reply_text("Admins only.")
        return
    args = (update.message.text or "").split(maxsplit=1)
    if len(args) < 2:
        await update.effective_message.reply_text("Usage: /deladmin <telegram_id>")
        return
    try:
        target = int(args[1])
    except ValueError:
        await update.effective_message.reply_text("telegram_id must be an integer.")
        return
    await remove_admin(target)
    await update.effective_message.reply_text(f"Removed admin: {target}")

async def admins_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await is_admin(update.effective_user.id):
        await update.effective_message.reply_text("Admins only.")
        return
    ids = await list_admins()
    if not ids:
        await update.effective_message.reply_text("No admins set.")
    else:
        await update.effective_message.reply_text("Admins:\n" + "\n".join(str(i) for i in ids))

async def menu_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if ALLOWED_CHAT and str(update.effective_chat.id) != str(ALLOWED_CHAT):
        await update.callback_query.answer('Not allowed in this chat.', show_alert=True)
        return
    q = update.callback_query; await q.answer()
    user = update.effective_user; s = _get_session(user.id)
    s.op = q.data; s.files.clear(); s.captions.clear()
    prompts = {
        'op_vv': 'Send 2+ videos in order. Then /done',
        'op_aa': 'Send 2+ audio files. Then /done',
        'op_vs': 'Send a video and a subtitle file (.srt/.ass/.vtt). Then /done',
        'op_va': 'Send a video and an audio file. Then /done',
    }
    await q.edit_message_text(prompts.get(s.op, 'Send files, then /done'))

async def _download_to_s3(bot, tg_file_id: str, key_prefix: str) -> str:
    file = await bot.get_file(tg_file_id)
    key = f"{key_prefix}/{uuid.uuid4().hex}"
    import s3_io
    async with httpx.AsyncClient(timeout=None) as client:
        async with client.stream('GET', file.file_path) as r:
            r.raise_for_status()
            def gen():
                for chunk in r.iter_bytes():
                    if chunk:
                        yield chunk
            s3_io.multipart_uploader(S3_BUCKET, key, gen())
    return key

async def collect_files(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if ALLOWED_CHAT and str(update.effective_chat.id) != str(ALLOWED_CHAT):
        return
    user = update.effective_user
    s = _get_session(user.id)
    if not s.op:
        await update.effective_message.reply_text('Use /start to choose an operation first.')
        return
    m = update.message
    tg_file = None
    filename = None
    if m.document: tg_file, filename = m.document.file_id, (m.document.file_name or 'file.bin')
    elif m.video: tg_file, filename = m.video.file_id, (m.video.file_name or 'video.mp4')
    elif m.audio: tg_file, filename = m.audio.file_id, (m.audio.file_name or 'audio.m4a')
    elif m.voice: tg_file, filename = m.voice.file_id, 'voice.ogg'
    else: return

    key = await _download_to_s3(context.bot, tg_file, key_prefix=str(user.id))
    s.files.append((tg_file, key))
    s.captions.append(m.caption or '')
    await update.effective_message.reply_text(f'Queued: {filename}')

async def done(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if ALLOWED_CHAT and str(update.effective_chat.id) != str(ALLOWED_CHAT):
        return
    user = update.effective_user
    s = _get_session(user.id)
    if not s.op:
        await update.effective_message.reply_text('Use /start to choose an operation first.')
        return
    files = [k for _, k in s.files]
    if s.op in ('op_vv', 'op_aa') and len(files) < 2:
        await update.effective_message.reply_text('Send at least TWO files, then /done.')
        return
    if s.op in ('op_vs', 'op_va') and len(files) != 2:
        await update.effective_message.reply_text('Send exactly TWO files, then /done.')
        return
    if not (await is_authorised(user.id) or await is_admin(user.id)):
        await update.effective_message.reply_text('Not authorised. Use /authorise <telegram_id> <gplinks_token>.')
        return

    await update.effective_message.reply_text('Merging…')

    input_urls = [presign_get(S3_BUCKET, k, 3600) for k in files]
    out_key = f"{user.id}/out/{uuid.uuid4().hex}.mkv"

    if s.op == 'op_aa':
        cmd = await audio_audio_to_stdout(input_urls)
    elif s.op == 'op_vv':
        cmd = await video_video_to_stdout(input_urls)
    elif s.op == 'op_vs':
        a, b = input_urls
        if any(b.lower().endswith(ext) for ext in ('.srt','.ass','.vtt')):
            cmd = await video_subtitle_to_stdout(a, b)
        else:
            cmd = await video_subtitle_to_stdout(b, a)
    elif s.op == 'op_va':
        a, b = input_urls
        cmd = await video_audio_to_stdout(a, b)
    else:
        await update.effective_message.reply_text('Unknown operation.')
        return

    import s3_io
    proc = await asyncio.create_subprocess_shell(
        cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )

    async def part_iter():
        assert proc.stdout is not None
        while True:
            chunk = await proc.stdout.read(1024 * 512)
            if not chunk:
                break
            yield chunk

    loop = asyncio.get_event_loop()
    s3_task = loop.run_in_executor(None, s3_io.multipart_uploader, S3_BUCKET, out_key, part_iter())

    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        await update.effective_message.reply_text(f"FFmpeg failed: {stderr.decode('utf-8','ignore')[:800]}")
        return
    await s3_task

    auth = await get_authorisation(user.id)
    get_url = s3_io.presign_get(S3_BUCKET, out_key, 3600)
    short_url = await shorten_with_gplinks(auth.get('gplinks_token',''), get_url)
    await update.effective_message.reply_text(f'Your file is ready:\n{short_url}')

    # Bot upload (<=2GB). If fails and Premium present, try Pyrogram.
    try:
        await update.effective_message.reply_document(get_url, filename='merged.mkv')
    except Exception:
        pyro = get_pyro()
        if pyro is not None:
            try:
                if not pyro.is_connected:
                    await pyro.start()
                await pyro.send_document(user.id, get_url, file_name='merged.mkv')
                await update.effective_message.reply_text('Sent via Premium uploader (Pyrogram).')
            except Exception as pe:
                await update.effective_message.reply_text(f'Could not upload via Pyrogram: {pe}')
        else:
            await update.effective_message.reply_text('Upload via bot failed (possibly >2GB). Use the link above.')

    _sessions.pop(user.id, None)

async def ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.effective_message.reply_text('pong')

def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit('Missing BOT_TOKEN')
    if not S3_BUCKET:
        raise SystemExit('Missing S3_BUCKET and AWS credentials')

    app = Application.builder().token(BOT_TOKEN).build()
    # seed admins from ADMINS env
    async def seed_admins():
        ids = [s.strip() for s in ADMINS_ENV.split(',') if s.strip()]
        for s in ids:
            try:
                await add_admin(int(s))
            except Exception:
                pass
    app.create_task(seed_admins())
    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('help', help_cmd))
    app.add_handler(CommandHandler('authorise', authorise_cmd))
    app.add_handler(CommandHandler('status', status_cmd))
    app.add_handler(CommandHandler('addadmin', addadmin_cmd))
    app.add_handler(CommandHandler('deladmin', deladmin_cmd))
    app.add_handler(CommandHandler('admins', admins_cmd))
    app.add_handler(CommandHandler('done', done))
    app.add_handler(CommandHandler('ping', ping))
    app.add_handler(CallbackQueryHandler(menu_choice))
    app.add_handler(MessageHandler(filters.VIDEO | filters.Document.ALL | filters.AUDIO | filters.VOICE, collect_files))
    app.run_polling(close_loop=False)

if __name__ == '__main__':
    main()
