#!/bin/sh
set -e

# During development and testing, we should be able to connect to
# services installed on "localhost" from the container. To allow this,
# we find the IP address of the docker host, and then for each
# variable name in "$SUBSTITUTE_LOCALHOST_IN_VARS", we substitute
# "localhost" with that IP address.
host_ip=$(ip route show | awk '/default/ {print $3}')
for envvar_name in $SUBSTITUTE_LOCALHOST_IN_VARS; do
    eval envvar_value=\$$envvar_name
    if [[ -n "$envvar_value" ]]; then
        eval export $envvar_name=$(echo "$envvar_value" | sed -E "s/(.*@|.*\/\/)localhost\b/\1$host_ip/")
    fi
done

# This function tries to connect to the RabbitMQ server with exponential
# backoff. This is necessary during development, because the RabbitMQ server
# might not be running yet when this script executes.
wait_for_rabbitmq() {
    local retry_after=1
    local time_limit=$(($retry_after << 5))
    local error_file="$APP_ROOT_DIR/rabbitmq-connect.error"
    echo -n 'Waiting for the RabbitMQ server ...'
    while [[ $retry_after -lt $time_limit ]]; do
        if python -m rmq_connect "$1" &>$error_file; then
            echo ' done.'
            return 0
        fi
        sleep $retry_after
        retry_after=$((2 * retry_after))
    done
    echo
    cat "$error_file"
    return 1
}

case $1 in
    test)
        # Do not run this in production!
        wait_for_rabbitmq "$PROTOCOL_BROKER_URL"
        exec pytest tests
        ;;
    swpt-server)
        exec "$@"
        ;;
    swpt-client)
        exec "$@"
        ;;
    *)
        exec "$@"
        ;;
esac
