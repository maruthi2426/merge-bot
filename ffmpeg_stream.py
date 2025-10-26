import shlex
from typing import List

def _q(s: str) -> str:
    return shlex.quote(s)

async def audio_audio_to_stdout(input_urls: List[str]) -> str:
    inputs = ' '.join(f'-i {_q(u)}' for u in input_urls)
    return f"ffmpeg -nostdin -hide_banner -loglevel error -y {inputs} -filter_complex concat=n={len(input_urls)}:v=0:a=1 -c:a aac -f matroska -"

async def video_video_to_stdout(input_urls: List[str]) -> str:
    v = ''.join(f'[{i}:v:0]' for i in range(len(input_urls)))
    a = ''.join(f'[{i}:a:0]' for i in range(len(input_urls)))
    filt = f"{v}{a}concat=n={len(input_urls)}:v=1:a=1[v][a]"
    inputs = ' '.join(f'-i {_q(u)}' for u in input_urls)
    return (
        f"ffmpeg -nostdin -hide_banner -loglevel error -y {inputs} "
        f"-filter_complex {_q(filt)} -map [v] -map [a] -c:v libx264 -preset veryfast -crf 23 -c:a aac -f matroska -"
    )

async def video_subtitle_to_stdout(video_url: str, subs_url: str) -> str:
    return (
        f"ffmpeg -nostdin -hide_banner -loglevel error -y -i {_q(video_url)} -i {_q(subs_url)} "
        f"-c copy -c:s srt -f matroska -"
    )

async def video_audio_to_stdout(video_url: str, audio_url: str) -> str:
    return (
        f"ffmpeg -nostdin -hide_banner -loglevel error -y -i {_q(video_url)} -i {_q(audio_url)} "
        f"-map 0:v:0 -map 1:a:0 -c:v copy -c:a aac -shortest -f matroska -"
    )
