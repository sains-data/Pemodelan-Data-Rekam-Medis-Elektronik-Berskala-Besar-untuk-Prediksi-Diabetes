# Sistem Prediksi Diabetes Berbasis Big Data

Sistem terintegrasi untuk prediksi diabetes menggunakan teknologi big data seperti Apache Spark, Hadoop, dan Airflow.

## Gambaran Umum

Sistem ini merupakan implementasi end-to-end pipeline prediksi diabetes yang mencakup:
- Ingest data otomatis dari berbagai sumber
- Validasi dan transformasi data (ETL)
- Training dan evaluasi model machine learning
- Prediksi dan reporting

## Arsitektur

Sistem mengadopsi arsitektur lambda dengan layer:
- **Bronze Layer**: Data mentah dari berbagai sumber
- **Silver Layer**: Data bersih dan terstandarisasi
- **Gold Layer**: Data siap analisis dan hasil prediksi

## Komponen Teknologi

- **Apache Airflow**: Orkestrasi workflow dan penjadwalan
- **Apache Spark**: Pemrosesan data dan machine learning
- **Apache Hadoop**: Penyimpanan data (HDFS)
- **Apache Hive**: Data warehousing
- **Apache Superset**: Visualisasi dan dashboarding
- **Docker**: Kontainerisasi seluruh layanan

## Struktur Proyek

```
/
├── docker-compose.yml         # Definisi infrastruktur container
├── airflow/
│   └── dags/
│       └── dag_diabetes.py    # DAG untuk workflow prediksi diabetes
├── data/
│   ├── bronze/                # Data mentah
│   ├── silver/                # Data bersih terstandarisasi
│   ├── gold/                  # Hasil prediksi dan visualisasi
│   └── logs/                  # Log proses
└── scripts/
    ├── ingest_data.sh         # Script ingest data
    ├── validate_data.py       # Validasi kualitas data
    ├── etl_spark_job.py       # ETL transformasi data
    ├── predict_diabetes.py    # Model training dan prediksi
    └── generate_report.py     # Pembuatan laporan dan visualisasi
```

## Alur Pipeline

1. **Ingest Data**: Data diambil dari berbagai sumber dan disimpan di bronze layer
2. **Validasi Data**: Validasi kualitas data untuk memastikan kelengkapan dan konsistensi
3. **ETL**: Transformasi dan standardisasi data untuk pemodelan
4. **Prediksi**: Training dan evaluasi model, kemudian prediksi pada data baru
5. **Reporting**: Pembuatan laporan dan visualisasi hasil

## Cara Menjalankan

### Prasyarat
- Docker dan Docker Compose
- Minimal 8GB RAM untuk menjalankan seluruh layanan

### Inisialisasi

1. Clone repository
```bash
git clone <repository-url> diabetes-prediction
cd diabetes-prediction
```

2. Jalankan infrastruktur:
```bash
docker-compose up -d
```

3. Buka web interface Airflow:
```
http://localhost:8089
```

4. Aktifkan DAG `dag_diabetes_prediction` di interface Airflow

### Monitoring

- **Airflow UI**: http://localhost:8089 (monitoring pipeline)
- **Spark UI**: http://localhost:8080 (monitoring Spark jobs)
- **HDFS UI**: http://localhost:9870 (monitoring storage)
- **Superset**: http://localhost:8081 (visualisasi dan dashboards)

## Skalabilitas

Sistem ini dirancang untuk skala besar:
- Pemrosesan data paralel dengan Spark
- Penyimpanan terdistribusi dengan HDFS
- Dapat ditambahkan node worker untuk meningkatkan kapasitas

## Pengembangan Lanjutan

- Integrasi dengan real-time data streaming
- Penambahan fitur dari sumber data wearable/IoT
- A/B testing berbagai algoritma ML
- Implementasi MLOps dan monitoring model

## Pemeliharaan

- Log tersimpan di `data/logs/`
- Monitoring resource menggunakan Docker stats
- Backup data gold dan model tersedia di `data/backup/`

## Lisensi

MIT

## Kontributor

- Kelompok 8 - Kelas Big Data