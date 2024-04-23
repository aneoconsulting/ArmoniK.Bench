#!/bin/bash

# Function to log messages
log() {
    echo -e "[INFO] $(date +"%Y-%m-%d %H:%M:%S") - $1"
}

# Function to log errors and exit
error() {
    echo "[ERROR] $(date +"%Y-%m-%d %H:%M:%S") - $1"
    exit 1
}

# Stop on error
set -e

# VARIABLES
LOCAL_ENV_NAME=armonik-bench
VERSION=composer-2.6.6-airflow-2.7.3


install_composer_dev() {
    log "Installing composer-dev..."

    git clone https://github.com/GoogleCloudPlatform/composer-local-dev.git || error "Failed to clone composer-dev repo."
    pip install composer-local-dev/ || error "Failed to install composer-dev."

    log "Cleaning up..."
    rm -rf composer-local-dev
}

setup_dev_composer() {
    log "Creating composer-dev environment..."
    composer-dev create $LOCAL_ENV_NAME \
        --from-image-version $VERSION \
        --port 8080 \
        --dags-path ./src/airflow/dags \
        --location us-central1 \
        --project project_id || error "Failed to create composer-dev environment."

    log "Generate requirements.txt file"
    python gen_requirements.py --sections airflow,tests

    log "Copying 'requirements.txt' and 'variables.env' into composer environment"
    cp requirements.txt composer/$LOCAL_ENV_NAME/requirements.txt
    cp variables.env composer/$LOCAL_ENV_NAME/variables.env

    log "Copying tests into composer environment"
    mkdir -p composer/$LOCAL_ENV_NAME/data/tests
    cp -r tests/airflow composer/armonik-bench/data/tests
}

goodbye() {
    log "To launch the environment run the following command:\n\n\tcomposer-dev restart ${LOCAL_ENV_NAME}"
}

install_composer_dev
setup_dev_composer
goodbye
