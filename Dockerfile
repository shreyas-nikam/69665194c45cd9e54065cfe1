# Use Python base image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt /app/

# Install wkhtmltopdf and its required system dependencies
# Added: libxrender1, libxext6, and fonts to prevent PDF generation errors
RUN apt-get update && apt-get install -y \
    wget \
    && wget https://github.com/wkhtmltopdf/packaging/releases/download/0.12.6.1-3/wkhtmltox_0.12.6.1-3.bookworm_amd64.deb \
    && apt-get install -y ./wkhtmltox_0.12.6.1-3.bookworm_amd64.deb \
    && rm wkhtmltox_0.12.6.1-3.bookworm_amd64.deb \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy the rest of the application code
COPY . /app

# Set the port number
ENV PORT=8501

# Expose the port (Note: EXPOSE is just documentation, it doesn't actually map ports)
EXPOSE $PORT

# Run Streamlit
# Added: --server.address=0.0.0.0 to ensure external accessibility
CMD ["bash", "-c", "streamlit run app.py --server.port=$PORT --server.address=0.0.0.0 --server.headless=true"]