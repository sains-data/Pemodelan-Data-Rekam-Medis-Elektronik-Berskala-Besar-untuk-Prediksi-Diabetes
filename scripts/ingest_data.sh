#!/bin/bash

# Set up error handling
set -e
set -o pipefail

# Konfigurasi
BRONZE_DIR="/usr/local/airflow/data/bronze"
LOG_DIR="/usr/local/airflow/data/logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${LOG_DIR}/ingest_data_${TIMESTAMP}.log"
DATA_DATE=$(date +%F)

# Memastikan direktori log tersedia
mkdir -p ${LOG_DIR}

# Function untuk logging
log() {
  local message="[$(date +"%Y-%m-%d %H:%M:%S")] $1"
  echo "$message" | tee -a ${LOG_FILE}
}

# Function untuk validasi file hasil download
validate_file() {
  local file=$1
  local min_size=$2
  
  if [ ! -f "$file" ]; then
    log "ERROR: File $file tidak ditemukan"
    return 1
  fi
  
  local size=$(wc -c < "$file")
  if [ "$size" -lt "$min_size" ]; then
    log "ERROR: File $file terlalu kecil (${size} bytes), kemungkinan corrupt"
    return 1
  fi
  
  log "INFO: File $file valid dengan ukuran ${size} bytes"
  return 0
}

# Function untuk handling error
handle_error() {
  log "ERROR: Proses ingest data gagal pada baris $1"
  exit 1
}

# Set up error trap
trap 'handle_error $LINENO' ERR

# Mulai proses
log "INFO: Memulai proses ingest data"

# Memastikan direktori bronze tersedia
log "INFO: Memastikan direktori bronze tersedia"
mkdir -p ${BRONZE_DIR}

# Unduh dataset dari sumber eksternal dengan retry dan timeout
log "INFO: Mengunduh dataset Pima Indians Diabetes"
curl --retry 3 --retry-delay 5 --max-time 30 --silent --show-error \
  -o "${BRONZE_DIR}/pima_${DATA_DATE}.csv" \
  https://raw.githubusercontent.com/sains-data/Pemodelan-Data-Rekam-Medis-Elektronik-Berskala-Besar-untuk-Prediksi-Diabetes/refs/heads/main/data/raw/diabetes.csv\
  >> ${LOG_FILE} 2>&1

# Validasi file pima
validate_file "${BRONZE_DIR}/pima_${DATA_DATE}.csv" 4000

# Mencoba mengunduh data wearable jika tersedia
log "INFO: Mencoba mengunduh dataset wearable (opsional)"
curl --retry 3 --retry-delay 5 --max-time 30 --silent --show-error \
  -o "${BRONZE_DIR}/wearable_${DATA_DATE}.csv" \
  https://raw.githubusercontent.com/sains-data/Pemodelan-Data-Rekam-Medis-Elektronik-Berskala-Besar-untuk-Prediksi-Diabetes/refs/heads/main/data/raw/personal_health_data.csv \
  >> ${LOG_FILE} 2>&1 || log "WARNING: Dataset wearable tidak tersedia, melanjutkan tanpa wearable data"

# Check if we were able to download wearable data
if [ -f "${BRONZE_DIR}/wearable_${DATA_DATE}.csv" ]; then
  validate_file "${BRONZE_DIR}/wearable_${DATA_DATE}.csv" 100 || \
    log "WARNING: File wearable tidak valid, melanjutkan tanpa data wearable"
fi

# Copy ke HDFS jika tersedia (opsional, tergantung setup)
# Uncomment jika menggunakan HDFS
# if command -v hdfs > /dev/null 2>&1; then
#   log "INFO: Copying files to HDFS"
#   hdfs dfs -mkdir -p /data/bronze
#   hdfs dfs -put -f ${BRONZE_DIR}/pima_${DATA_DATE}.csv /data/bronze/
#   
#   if [ -f "${BRONZE_DIR}/wearable_${DATA_DATE}.csv" ]; then
#     hdfs dfs -put -f ${BRONZE_DIR}/wearable_${DATA_DATE}.csv /data/bronze/
#   fi
# else
#   log "INFO: HDFS not available, skipping HDFS storage"
# fi

# Membuat file marker untuk menandakan proses ingest selesai
touch "${BRONZE_DIR}/_SUCCESS_${DATA_DATE}"

log "INFO: Proses ingest data selesai dengan sukses"
exit 0
