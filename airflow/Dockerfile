# VERSION 1.10.9
# AUTHOR: Matthieu "Puckel_" Roisil
# DESCRIPTION: Basic Airflow container
# BUILD: docker build --rm -t puckel/docker-airflow .
# SOURCE: https://github.com/puckel/docker-airflow

FROM puckel/docker-airflow:1.10.9


EXPOSE 8089 5555 8793

USER airflow

# Install Python dependencies for ML and reporting
RUN pip install pandas numpy scikit-learn matplotlib seaborn fpdf
