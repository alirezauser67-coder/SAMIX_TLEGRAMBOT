# SAMIX Telegram Bot

> 🇮🇷 **نسخه فارسی:** [README.md](README.md)

A self-hosted Telegram relay bot with a **web panel** and a hidden **Telegram control panel**. Messages sent to the bot are relayed to the owner (and allowed operators), who can reply right from the web UI or from Telegram.

## Features

- Relay messages from users to the owner and approved operators
- Web dashboard with login password
- Hidden Telegram panel via `/samx` (operators) and `/panel` (owner only)
- Access control: allow / ban / revoke operators
- Auto-reply, broadcast, scheduled messages
- Media support (photos, video, voice, documents, stickers, ...)
- SQLite storage (no external database needed)

## Requirements

- Python 3.9+
- A Telegram bot token from [@BotFather](https://t.me/BotFather)

## Setup

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Copy the example env file and fill in your values:

   ```bash
   cp .env.example .env
   ```

3. Edit `.env`:

   | Variable        | Description                                            |
   | --------------- | ------------------------------------------------------ |
   | `BOT_TOKEN`     | Bot token from BotFather                               |
   | `ADMIN_CHAT_ID` | Your numeric Telegram user ID (the owner)              |
   | `WEB_PASSWORD`  | Password for the web panel                             |
   | `WEB_HOST`      | Bind address (default `0.0.0.0`)                       |
   | `WEB_PORT`      | Web port (default `8080`, or `PORT` env on hosts)      |
   | `DATA_DIR`      | Folder for the database/data (default: project folder) |
   | `SAVE_HISTORY`  | `1` to keep chat history, `0` for RAM only             |

4. Run the bot:

   ```bash
   python relay_web.py
   ```

5. Open the web panel at `http://localhost:8080` and log in with `WEB_PASSWORD`.

## Telegram panel

- Send `/samx` to the bot to open the operator panel.
- The owner can use `/panel` for full control (users, settings, broadcast, schedules).

## Deploy on Railway

1. Push this repo to GitHub.
2. Create a new project on [Railway](https://railway.app) from the repo.
3. Add the environment variables from the table above in the Railway dashboard.
4. Set `DATA_DIR=/data` and attach a volume at `/data` for persistent storage.

## Security

- **Never commit your `.env`** — it is already in `.gitignore`.
- Keep `BOT_TOKEN` and `WEB_PASSWORD` secret.
- Put the web panel behind HTTPS / a reverse proxy when exposed publicly.

## License

MIT
