#!/bin/bash
# Shared helpers for AWS shell scripts

COMMON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${COMMON_DIR}/.." && pwd)"
DEFAULT_ENV_FILE="${PROJECT_ROOT}/.env"

load_env() {
    local env_file="${1:-${DEFAULT_ENV_FILE}}"

    if [ ! -f "${env_file}" ]; then
        echo "ERROR: .env file not found at ${env_file}. Run setup-aws.sh first." >&2
        exit 1
    fi

    # shellcheck disable=SC1090
    source "${env_file}"
}

require_env() {
    local missing=()

    for var in "$@"; do
        if [ -z "${!var:-}" ]; then
            missing+=("${var}")
        fi
    done

    if [ "${#missing[@]}" -ne 0 ]; then
        echo "ERROR: Missing required environment variable(s): ${missing[*]}" >&2
        exit 1
    fi
}

require_command() {
    for cmd in "$@"; do
        if ! command -v "${cmd}" >/dev/null 2>&1; then
            echo "ERROR: Required command '${cmd}' not found in PATH." >&2
            exit 1
        fi
    done
}

aws_ecr_login() {
    require_env AWS_REGION AWS_ACCOUNT_ID
    require_command aws docker

    aws ecr get-login-password --region "${AWS_REGION}" | \
        docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
}
