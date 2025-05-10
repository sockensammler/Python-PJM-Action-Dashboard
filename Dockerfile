FROM python:3.11-slim

# Optional: Systempakete installieren, falls nötig (z. B. für pandas dependencies)
RUN apt-get update && apt-get install -y build-essential

WORKDIR /app

# Kopiere requirements und installiere vorher
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Jetzt den Code kopieren
COPY . .

# Exponiere den Port für Streamlit
EXPOSE 8501

# Starte Streamlit sauber
CMD ["python", "-m", "streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
