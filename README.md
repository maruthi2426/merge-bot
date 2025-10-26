# Heroku Telegram Merge Bot (Advanced)


## Admin features
- Multiple admins via MongoDB (`/addadmin <id>`, `/deladmin <id>`, `/admins`).
- Admins can run `/authorise <target_id>` **without** a token; the bot will fall back to `MASTER_GPLINKS_API`.
- Admins themselves skip verification when merging.
- Seed admins with `ADMINS` env var (comma-separated Telegram user IDs).
