#!/usr/bin/env bash
# ============================================================
# 回到基线（执行方案 §4.2）
#
# 每条实验前都跑一次。不跑就开工，等于承认「固定条件」是嘴上说说，
# 后面 E1–E5 的对照数据全部作废。
#
# 用法：bash deploy/reset.sh
# ============================================================
set -euo pipefail

cd "$(dirname "$0")"

DB_PASS="${DB_PASS:-campusslot}"
DB_PORT="${DB_PORT:-3307}"

echo ">>> 1/5 销毁并重建容器（含数据卷）"
docker compose down -v
docker compose up -d

echo ">>> 2/5 等待 MySQL 就绪"
for _ in $(seq 1 60); do
  if docker exec cs-mysql mysqladmin ping -h 127.0.0.1 -uroot -p"$DB_PASS" --silent >/dev/null 2>&1; then
    echo "    MySQL ready"
    break
  fi
  sleep 2
done

echo ">>> 3/5 等待 Broker 注册到 NameServer"
for _ in $(seq 1 60); do
  if docker exec cs-broker sh mqadmin clusterList -n namesrv1:9876 >/dev/null 2>&1; then
    echo "    Broker ready"
    break
  fi
  sleep 2
done
sleep 3

echo ">>> 4/5 初始化 Topic（显式指定队列数）"
bash topic-init.sh

echo ">>> 5/5 清空 Redis（库存 key 必须从 DB 重新灌，不能留着上一轮的残值）"
docker exec cs-redis redis-cli FLUSHALL

cat <<EOF

基线就绪：
  MySQL     127.0.0.1:${DB_PORT}  db=campusslot user=root pass=${DB_PASS}
  Redis     127.0.0.1:6379        policy=noeviction
  Proxy     127.0.0.1:8081        (客户端连这里，不是 9876)
  Dashboard http://127.0.0.1:8180
  Grafana   http://127.0.0.1:3000
  Prom      http://127.0.0.1:9090

提醒：Redis 里的库存要在资源发布时由 com.campusslot.app.resource 灌入；
当前还没有该逻辑，D2 之前如需手工验证 Lua，可执行
  docker exec cs-redis redis-cli SET quota:stock:1 200
EOF
