import httpx

async def shorten_with_gplinks(api_token: str, url: str) -> str:
    if not api_token:
        return url
    q = {'api': api_token, 'url': url}
    async with httpx.AsyncClient(timeout=None) as client:
        r = await client.get('https://gplinks.in/api', params=q)
        r.raise_for_status()
        data = r.json()
    for k in ('shortenedUrl', 'shortenUrl', 'shortlink', 'short'):
        if k in data and data[k]:
            return data[k]
    if 'url' in data and isinstance(data['url'], str):
        return data['url']
    return url
