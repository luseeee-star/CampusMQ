#!/usr/bin/env bash
# ============================================================
# Topic 初始化
#
# 这一步必须显式做，不能依赖自动创建：自动创建的 Topic 只有 4 个队列，
# 而队列数就是消费并行度的上限。压测时如果消费 TPS 上不去，
# 第一件事就是回来查这里——而不是去调消费者线程数。
#
# v2.0 只剩三个 Topic（不做抽签、对账走定时任务），见 MqTopic.java。
# ============================================================
set -euo pipefail

NAMESRV="${NAMESRV:-namesrv1:9876}"
CLUSTER="${CLUSTER:-DefaultCluster}"
CONTAINER="${CONTAINER:-cs-broker}"

mq() {
  docker exec "$CONTAINER" sh mqadmin "$@"
}

create_topic() {
  local topic=$1 queues=$2
  echo ">>> topic ${topic} (queues=${queues})"
  mq updateTopic -n "$NAMESRV" -c "$CLUSTER" -t "$topic" -r "$queues" -w "$queues"
}

# 异步落库 + 锁定通知（两个消费组各自收全量）
create_topic quota_claimed 16
# 30 分钟未确认释放的定时消息（SYS-1）；队列数不够会直接吃掉 NFR-5 的 1s 误差指标
create_topic quota_release_delay 8
# 补位定向通知（R-6）
create_topic quota_backfill 4

echo
echo ">>> 校验队列数（必须是上面写的数字，不是 4）"
# 不能用 topicStatus 校验：它按位点统计，topic 里一条消息都没有时返回空表，
# 于是「队列数 16」和「topic 根本没建成」看起来一模一样。
# topicRoute 查的是路由元数据，刚建完、零消息也能读到准确队列数。
check_queues() {
  local topic=$1 want=$2 got
  got=$(mq topicRoute -n "$NAMESRV" -t "$topic" 2>/dev/null \
        | grep -m1 -o '"readQueueNums":[0-9]*' | cut -d: -f2)
  if [ "$got" = "$want" ]; then
    echo "    ${topic}  OK    read=write=${got}"
  else
    echo "    ${topic}  不匹配  实际=${got:-查不到}  期望=${want}"
  fi
}
check_queues quota_claimed 16
check_queues quota_release_delay 8
check_queues quota_backfill 4

echo
echo "Topic 初始化完成。Dashboard: http://127.0.0.1:8180"
echo "若某个 Topic 的队列数仍为 4，说明 updateTopic 没生效（多半是 -c 集群名不匹配），"
echo "改用 -b broker 地址重试：docker exec cs-broker sh mqadmin updateTopic -b broker:10911 -t <topic> -r 16 -w 16"
