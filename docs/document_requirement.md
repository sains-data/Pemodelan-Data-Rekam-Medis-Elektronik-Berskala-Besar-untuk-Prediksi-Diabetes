# Spesifikasi Proyek Big Data: Pemodelan Data Rekam Medis Elektronik Berskala Besar untuk Prediksi Diabetes Menggunakan Apache Spark

## Disusun oleh

**Kelompok 8 RA**

* Eggi Satria (122450040)
* Ganiya Syazwa (122450073)
* Sahid Maulana (122450109)
* Ima Alifah Izati Zalfa (121450140)

Institut Teknologi Sumatera, Lampung Selatan
Mei 2025

---

## Daftar Isi

1. [Pendahuluan](#1-pendahuluan)
2. [Deskripsi Umum](#2-deskripsi-umum)
3. [Spesifikasi Proyek](#3-spesifikasi-proyek)
4. [Metode Umum](#4-metode-umum)
5. [Lampiran](#5-lampiran)

---

## 1. Pendahuluan

### 1.1 Tujuan Dokumen

Dokumen ini menyediakan spesifikasi teknis dan fungsional untuk proyek big data bertajuk *"Pemodelan Data Rekam Medis Elektronik Berskala Besar untuk Prediksi Diabetes Menggunakan Apache Spark"*. Tujuan utamanya adalah memberikan panduan menyeluruh dalam pelaksanaan ETL, pelatihan model ML, dan visualisasi hasil analitik berbasis arsitektur data lake.

### 1.2 Lingkup Sistem

Sistem mencakup proses pengumpulan, pemrosesan, penyimpanan, analitik, dan visualisasi data RME. Fokus pada penerapan arsitektur **Data Lake** berbasis HDFS, menggunakan Apache Spark dan Apache NiFi, dengan visualisasi melalui **Grafana**.

### 1.3 Latar Belakang

Rekam Medis Elektronik (RME) menghasilkan data dalam volume besar dan beragam format. Diperlukan pendekatan data engineering modern berbasis data lake untuk ingestion, pemrosesan, dan analitik secara efisien dan scalable.

---

## 2. Deskripsi Umum

### 2.1 Perspektif Sistem

Sistem dirancang sebagai **data lake architecture** menggunakan Apache stack: Apache NiFi (ingest), Apache HDFS (storage), Apache Spark (ETL & ML), Apache Hive (query layer), Apache Airflow (orchestration), dan **Grafana** (visualisasi real-time). Semua layanan terintegrasi dalam lingkungan Docker multi-container.

### 2.2 Fungsi Sistem Utama

* **Ingest Data** dari berbagai sumber (rumah sakit, wearable) menggunakan Apache NiFi.
* **Simpan Data** ke HDFS sebagai data lake zone (Bronze → Silver → Gold).
* **Transformasi & Analitik** menggunakan Apache Spark dan Spark MLlib.
* **Query Layer** dengan Apache Hive.
* **Visualisasi** real-time dengan Grafana melalui Prometheus & Spark metrics.
* **Orkestrasi & Monitoring** dengan Apache Airflow dan Grafana.

### 2.3 Karakteristik Pengguna

* **Tenaga Medis**: Melihat insight risiko secara real-time melalui dashboard Grafana.
* **Data Scientist**: Mengevaluasi performa model dan tren risiko.
* **Data Engineer**: Menjaga pipeline ingestion dan analitik tetap berjalan dengan integrasi penuh antar Apache services.

---

## 3. Spesifikasi Proyek

### 3.1 Ringkasan

Menggunakan pendekatan **Apache Data Lake** dengan Medallion Architecture (Bronze → Silver → Gold). Semua komponen dijalankan sebagai Docker container dan saling terintegrasi. Visualisasi dilakukan dengan **Grafana** yang terhubung ke Prometheus dan Spark.

### 3.2 Metode Proyek: Waterfall

Tahapan:

1. **Kebutuhan**: Definisikan kebutuhan ingestion, transformasi, prediksi, dan visualisasi
2. **Desain**: Arsitektur microservices Apache stack
3. **Implementasi**: Docker Compose multi-container
4. **Pengujian**: Cek validasi pipeline, performa dan akurasi
5. **Visualisasi & Insight**: Dashboard real-time dan historis di Grafana

### 3.3 Studi Kasus

* Ingest data CSV wearable dan klinis ke HDFS menggunakan Apache NiFi
* Gunakan Apache Spark untuk pembersihan dan penggabungan data
* Latih model MLlib (LogReg, RandomForest)
* Kirim metrik training dan prediksi ke Prometheus dan tampilkan di Grafana

---

## 4. Metode Umum

### 4.1 Arsitektur Sistem
![Arsitektur Sistem](https://raw.githubusercontent.com/sains-data/Pemodelan-Data-Rekam-Medis-Elektronik-Berskala-Besar-untuk-Prediksi-Diabetes/main/docs/images/architecture.png)
Arsitektur sistem menggunakan pendekatan **Data Lake** dengan Medallion Architecture:
* **Bronze Zone**: Data mentah dari berbagai sumber (CSV, JSON, Parquet) disimpan di HDFS.
* **Silver Zone**: Data dibersihkan, di-normalisasi, dan disimpan dalam format Parquet.
* **Gold Zone**: Data siap pakai untuk analitik dan model ML, termasuk fitur engineering.
* **Orkestrasi**: Apache Airflow mengelola alur kerja ETL dan ML.
* **Visualisasi**: Grafana menampilkan metrik dan dashboard real-time.
### 4.1.1 Komponen Sistem
* **Apache NiFi**: Untuk ingestion data dari berbagai sumber (rumah sakit, wearable).
* **Apache HDFS**: Sebagai data lake untuk menyimpan data mentah dan terstruktur.
* **Apache Spark**: Untuk ETL, pembersihan data, dan pelatihan model ML.
* **Apache Hive**: Sebagai query layer untuk akses data terstruktur.
* **Apache Airflow**: Untuk orkestrasi alur kerja ETL dan ML.
* **Grafana**: Untuk visualisasi metrik dan dashboard real-time.
### 4.1.2 Proses ETL
* **Ingest**: Data diambil dari sumber eksternal (rumah sakit, wearable) menggunakan Apache NiFi. Data disimpan dalam format mentah di HDFS (Bronze Zone).
* **Transformasi**: Menggunakan Apache Spark untuk membersihkan, menggabungkan, dan mengolah data. Data disimpan dalam format Parquet di Silver Zone.
* **Load**: Data yang telah dibersihkan dan diolah disimpan di Gold Zone, siap untuk analitik dan model ML.
### 4.1.3 Model ML
* **Pelatihan Model**: Menggunakan Spark MLlib untuk melatih model prediksi diabetes (misalnya, Logistic Regression, Random Forest).
* **Evaluasi Model**: Menggunakan metrik seperti akurasi, precision, recall, F1-score, dan AUC.
* **Penyimpanan Model**: Model yang telah dilatih disimpan dalam format yang dapat digunakan kembali, seperti PMML atau pickle.
### 4.1.4 Visualisasi
* **Grafana**: Digunakan untuk visualisasi metrik dan dashboard real-time.
    * Menggunakan datasource Prometheus untuk mengambil metrik dari Spark dan Airflow.
    * Dashboard mencakup:
    
        * Latensi pipeline ETL
        * Akurasi dan distribusi prediksi
        * Jumlah pasien berisiko tinggi
        * Heatmap tren glukosa mingguan
* **Prometheus**: Digunakan untuk mengumpulkan metrik dari Spark dan Airflow, yang kemudian ditampilkan di Grafana.
### 4.1.5 Orkestrasi
* **Apache Airflow**: Digunakan untuk mengelola alur kerja ETL dan ML.
    * DAG (Directed Acyclic Graph) untuk mengatur urutan tugas ingestion, transformasi, pelatihan model, dan evaluasi.
    * Monitoring dan logging untuk memastikan pipeline berjalan dengan baik.
### 4.1.6 Observabilitas
* **Prometheus**: Mengumpulkan metrik dari semua komponen sistem (Spark, Airflow, NiFi).
* **Grafana**: Menyediakan dashboard untuk memantau kesehatan sistem, latensi pipeline, dan performa model.
### 4.1.7 Pengujian
* **Unit Testing**: Setiap komponen ETL dan model ML diuji secara terpisah.
* **Integration Testing**: Pipeline ETL diuji secara end-to-end dengan data dummy.
* **Data Quality Tests**: Memastikan tidak ada data yang hilang, schema sesuai, dan outlier terdeteksi.
### 4.1.8 Deployment
* **Docker Compose**: Semua layanan (NiFi, Spark, Hive, Airflow, Grafana) dijalankan sebagai container Docker.
* **Makefile**: Skrip untuk memudahkan setup dan deployment lokal.
### 4.1.9 Dokumentasi
* **README.md**: Menyediakan petunjuk penggunaan, setup, dan kontribusi.

### 4.2 Revisi Visualisasi

Visualisasi tidak lagi menggunakan Superset, namun **Grafana**:

* Menggunakan datasource Prometheus
* Dashboards mencakup:

  * Latensi pipeline ETL
  * Akurasi dan distribusi prediksi
  * Jumlah pasien berisiko tinggi
  * Heatmap tren glukosa mingguan

---

## 5. Lampiran

### 5.1 Repositori Portofolio

[GitHub: sains-data/Prediksi-Diabetes](https://github.com/sains-data/Pemodelan-Data-Rekam-Medis-Elektronik-Berskala-Besar-untuk-Prediksi-Diabetes)

---

## Referensi

1. Sharma et al., Apache Spark for Analysis of EHR - 2023
2. Ebada et al., Spark on Streaming Health Data - 2022
3. Lian et al., Optimizing Spark for Healthcare - 2024
4. Ibrahim et al., Big Data Analytics for Diabetes Prediction - 2019
