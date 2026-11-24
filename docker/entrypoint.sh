#!/bin/sh
# 115 Bridge 容器入口：以 root 启动，按 PUID/PGID/UMASK 降权后执行命令。
# 只保证 /config 可写；不 chown /data（NAS 上已有文件属主各异），新建文件按 PUID/PGID/UMASK。
set -eu

PUID="${PUID:-1024}"
PGID="${PGID:-100}"
UMASK="${UMASK:-000}"

die() {
    echo "entrypoint: $*" >&2
    exit 2
}

case "$PUID" in '' | *[!0-9]*) die "PUID must be a number" ;; esac
case "$PGID" in '' | *[!0-9]*) die "PGID must be a number" ;; esac
# UMASK 必须是 3 或 4 位八进制数字（Python 侧还会再校验一次）
case "$UMASK" in
    [0-7][0-7][0-7] | [0-7][0-7][0-7][0-7]) ;;
    *) die "UMASK must be 3 or 4 octal digits (0-7)" ;;
esac

if [ "$(id -u)" != "0" ]; then
    # 已经以非 root 启动（例如 compose 里写了 user:），无法再降权，直接执行。
    umask "$UMASK"
    exec "$@"
fi

# 组：同 GID 已存在就复用，否则创建。
if ! getent group "$PGID" > /dev/null 2>&1; then
    groupadd -g "$PGID" bridge
fi
# 用户：同 UID 已存在就复用，否则创建（无登录 shell，家目录 /config）。
if ! getent passwd "$PUID" > /dev/null 2>&1; then
    useradd -u "$PUID" -g "$PGID" -M -d /config -s /usr/sbin/nologin bridge
fi

mkdir -p /config
chown -R "$PUID:$PGID" /config
chmod u+rwx /config

export HOME=/config
umask "$UMASK"
exec setpriv --reuid="$PUID" --regid="$PGID" --clear-groups -- "$@"
