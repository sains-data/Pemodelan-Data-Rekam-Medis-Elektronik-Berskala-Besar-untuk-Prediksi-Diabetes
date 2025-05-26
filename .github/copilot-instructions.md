
# 🧠 Copilot Workspace Instruction: Best Practices for Data Engineering Projects

This file provides guidelines and best practices for managing and maintaining a robust and scalable data engineering project workspace.

---

## 📁 1. Workspace Structure

```
/project-root/
│
├── data/
│   ├── bronze/         # Raw data ingested (CSV/JSON)
│   ├── silver/         # Cleaned & normalized data (Parquet)
│   └── gold/           # Aggregated, transformed & predicted data
│
├── scripts/
│   ├── ingestion/      # Apache NiFi processors or fetch scripts
│   ├── transform/      # PySpark transformation scripts
│   └── ml/             # Model training & evaluation scripts
│
├── airflow/
│   └── dags/           # Airflow DAG definitions
│
├── hive/
│   └── ddl/            # Hive table creation scripts
│
├── dashboard/
│   └── superset/       # Superset dashboard config & chart definitions
│       └── Grafana/    # Ganfari dashboard config & chart definitions
│
├── models/             # Trained ML models & pipeline metadata
├── logs/               # Structured logs for observability
└── README.md
```

---

## ⚙️ 2. ETL Best Practices

- Use **Apache NiFi** for flexible, visual data ingestion workflows.
- Design ETL in **3 stages**: Ingest → Transform → Load (Bronze → Silver → Gold).
- Keep ETL idempotent & monitorable (use checkpoints, logging).
- Store cleaned and structured data in **Parquet** format.

---

## 🧪 3. Testing Strategy

- **Unit Test** each transformation or cleaning step.
- **Integration Test** data pipeline end-to-end with dummy data.
- **Data Quality Tests**: check nulls, schema conformity, outlier bounds.

---

## 📊 4. Observability & Monitoring

- Use **Prometheus + Grafana** for cluster health and pipeline metrics.
- Enable **logging** for each step in ETL and model training (store in `/logs`).
- Track data freshness and SLA via Airflow sensors.

---

## 📦 5. Model Management

- Save models and preprocessing pipelines to `/models/` with versioning.
- Use **MLlib Pipelines** to encapsulate preprocessing and model stages.
- Log metrics: accuracy, precision, recall, F1, AUC.

---

## ✅ 6. Deployment & Automation

- Use **Docker Compose** to containerize all services.
- DAG scheduling with **Apache Airflow**.
- Prefer **Makefiles** or scripts to automate reproducible local setup.

---

## 📄 7. Documentation

- Use `README.md` to document pipeline flow, technologies, and usage.
- Keep `copilot_instruction.md` for onboarding new developers.
- Document each DAG, Spark job, and Hive table in Markdown or inline comments.
- Maintain a **CHANGELOG.md** for tracking changes and updates.
- Use **Jupyter Notebooks** for exploratory data analysis and visualization.
- Maintain a **Wiki** or **Confluence** page for high-level architecture and design decisions.
- Use **Markdown** for all documentation to ensure readability and version control compatibility.
- Include **data dictionaries** for each dataset, detailing schema, types, and descriptions.
- Use **GitHub Issues** or **Jira** for task tracking and feature requests.
- Maintain a **style guide** for code consistency (e.g., PEP 8 for Python).
- Use **GitHub Actions** or **GitLab CI** for continuous integration and deployment (CI/CD).
- Regularly update documentation to reflect changes in the codebase or architecture.
- Use **Sphinx** or **MkDocs** for generating static documentation from Markdown files.
---

> **Note**: Maintain clean commit history, use branches for features, and always write meaningful messages.

## 🛠️ 8. Tools & Technologies

- **Apache NiFi**: For data ingestion and ETL workflows.
- **Apache Spark**: For large-scale data processing and transformations.
- **Airflow**: For orchestrating complex data workflows.
- **Hive**: For data warehousing and SQL queries on big data.
- **Superset**: For data visualization and dashboarding.
- **Grafana**: For monitoring and observability and dashboard visualisasion.
- **Docker**: For containerization and environment management.
- **Prometheus**: For metrics collection and monitoring.
- **MLlib**: For machine learning tasks within Spark.
- **Jupyter Notebooks**: For interactive data analysis and exploration.
- **Git**: For version control and collaboration.
- **Make**: For build automation and task management.
