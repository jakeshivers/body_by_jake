#!/bin/sh
mc alias set local http://minio:9000 admin password
mc mb local/bbj-lakehouse
mc policy set public local/bbj-lakehouse
