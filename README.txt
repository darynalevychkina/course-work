Car Service Booking Bot

This repository contains a Telegram bot for booking appointments at a car service (СТО).  
The bot allows clients to register, link their vehicle (by VIN or license plate), choose a date and time, and create an appointment. Optionally, bookings can be synchronized with Google Calendar.

Getting Started

Follow the steps below to run the project locally.

1. Prerequisites
- Python 3.10+
- A Telegram bot token from @BotFather
- API keys for external vehicle services (e.g. Auto.dev, Baza-GAI)
- Google Cloud project with a Service Account and access to Google Calendar

Check your Python version:
python --version

2. Clone the repository
git clone https://github.com/<username>/<repo>.git
cd <repo>

4. Install dependencies
pip install -r requirements.txt

5. Configure environment variables
Create a file named .env in the project root (next to main.py):
BOT_TOKEN=your_telegram_bot_token

# Optional admin IDs (Telegram user IDs separated by comma)
ADMIN_IDS=123456789,987654321

# Timezone
TIMEZONE=Europe/Kyiv

# Directory for storing local receipts
RECEIPTS_DIR=./receipts

# Car check config
BAZAGAI_API_KEY=your_bazagai_api-key
AUTO_DEV_API_KEY=your_auto_dev_api_key=

# Google Calendar integration
GOOGLE_SERVICE_ACCOUNT_FILE=service-account.json
GOOGLE_CALENDAR_ID=your_calendar_id@group.calendar.google.com

If you use Google Calendar:
Download the service account JSON file from Google Cloud Console.
Save it in the project root (e.g. service-account.json).
Share your target Google Calendar with the service account e-mail (with “Make changes to events” permission).

6. Run the bot
Make sure your virtual environment is active and .env is configured, then run:
python main.py

If everything is configured correctly, you should see log messages like:
TIMEZONE in use: Europe/Kyiv
Receipts dir: ...
BazaGAI: mock=...
Bot started.

7. Test in Telegram
Open Telegram and find your bot by its username.
Press Start or send /start.
Use:
"Зареєструватися" to register as a new client (name, phone, vehicle).
"Зробити запис" to book a visit (date, time, reason).


