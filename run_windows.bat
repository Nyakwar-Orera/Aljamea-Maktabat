@echo off
echo 📚 Starting Maktabat al-Jamea Portal (Institutional Deployment)...

:: Check for environment variables
if not exist .env (
    echo ❌ ERROR: .env file not found. Please create one based on .env.example.
    pause
    exit /b
)

:: Activate Virtual Environment
if not exist venv (
    echo 📦 Creating virtual environment...
    python -m venv venv
)
call venv\Scripts\activate

:: Install/Update Dependencies
echo 💎 Checking dependencies...
pip install -r requirements.txt

:: Initialize/Migrate Metadata Database
echo 🏗️ Initializing database...
python appdata_init.py

:: Run with Waitress (Production-Grade WSGI)
echo 🚀 Launching Production Server on http://0.0.0.0:5000...
echo 🛡️  Waitress is serving the Aljamea Library Portal with security hardening active.
python app.py

pause
