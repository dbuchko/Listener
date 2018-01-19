# Listener
Sample RabbitMQ listener suitable for deployment to PCF.

Change the name of the RabbitMQ service in the manifest to match your own.

Heartbeat interval is controlled by the `HEARTBEAT_INTERVAL_SEC` environment variable.

Autorecovery is enabled.

Topology recovery can be disabled by setting `TOPOLOGY_RECOVERY_ENABLED` environment variable to `false`.
