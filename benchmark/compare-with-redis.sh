#!/usr/bin/env bash
# Compares recache (built from this checkout) with Redis 7.4 — default
# and with I/O threads — using the official redis-benchmark client.
#
#   ./benchmark/compare-with-redis.sh [rounds]
#
# Needs Docker. Every suite is run against all three servers back to
# back, in alternating order each round, so they share the same load
# conditions; the table shows the average over the rounds. Persistence
# is off on all servers. Results (raw CSV) are kept in
# benchmark/compare-results.csv.
set -euo pipefail

rounds="${1:-2}"
root="$(cd "$(dirname "$0")/.." && pwd)"
out="$root/benchmark/compare-results.csv"
net=recache-compare

cleanup() {
  docker rm -f compare-redis compare-redis-io compare-recache >/dev/null 2>&1 || true
  docker network rm "$net" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "Building recache image..." >&2
docker build -q -t recache:compare "$root" >/dev/null
docker pull -q redis:7.4 >/dev/null

cleanup
docker network create "$net" >/dev/null
docker run -d --name compare-redis --network "$net" redis:7.4 \
  redis-server --save "" --appendonly no >/dev/null
docker run -d --name compare-redis-io --network "$net" redis:7.4 \
  redis-server --save "" --appendonly no --io-threads 4 --io-threads-do-reads yes >/dev/null
docker run -d --name compare-recache --network "$net" \
  -e Recache__Port=6379 -e Recache__RequireAuth=false -e Recache__EnableFileLogging=false \
  recache:compare >/dev/null
sleep 5

echo "round,server,suite,test,rps,avg_ms,min_ms,p50_ms,p95_ms,p99_ms,max_ms" > "$out"

bench() { # round server suite redis-benchmark-args...
  local round=$1 server=$2 suite=$3; shift 3
  docker run --rm --network "$net" redis:7.4 redis-cli -h "$server" -p 6379 FLUSHALL >/dev/null
  docker run --rm --network "$net" redis:7.4 redis-benchmark -h "$server" -p 6379 --csv "$@" 2>/dev/null \
    | tr -d '\r' | grep -v '^"test"' | sed "s/^/$round,${server#compare-},$suite,/" >> "$out"
}

suite() { # round order name args...
  local round=$1 order=$2 name=$3; shift 3
  for server in $order; do bench "$round" "$server" "$name" "$@"; done
  echo "round $round: $name done" >&2
}

for round in $(seq 1 "$rounds"); do
  if (( round % 2 == 1 )); then
    order="compare-redis compare-redis-io compare-recache"
  else
    order="compare-recache compare-redis-io compare-redis"
  fi

  suite "$round" "$order" c50-P1  -c 50  -n 300000  -r 1000000 --threads 4 -t ping_mbulk,set,get,incr,lpush,rpop,sadd,hset,lrange_100,mset
  suite "$round" "$order" c50-P16 -c 50  -n 3000000 -r 1000000 --threads 4 -P 16 -t set,get,incr,lpush,hset,mset
  suite "$round" "$order" c200-P1 -c 200 -n 400000  -r 1000000 --threads 4 -t set,get
  suite "$round" "$order" c1-P1   -c 1   -n 50000   -r 1000000 -t set,get
done

awk -F, 'NR > 1 {
  gsub(/"/, "");
  key = $3 "|" $4;
  rps[key, $2] += $5; p99[key, $2] += $10; n[key, $2]++;
  if ($11 > worst[key, $2]) worst[key, $2] = $11;
  if (!(key in seen)) { seen[key] = 1; order[++k] = key }
}
END {
  printf "%-8s %-26s %10s %10s %10s %8s | %22s | %22s\n", "suite", "test", "redis", "redis-io", "recache", "rc/redis", "p99 ms r / io / rc", "max ms r / io / rc";
  for (i = 1; i <= k; i++) {
    key = order[i]; split(key, part, "|");
    r = rps[key, "redis"] / n[key, "redis"]; io = rps[key, "redis-io"] / n[key, "redis-io"]; rc = rps[key, "recache"] / n[key, "recache"];
    printf "%-8s %-26s %10.0f %10.0f %10.0f %7.2fx | %6.2f / %5.2f / %6.2f | %6.0f / %5.0f / %6.0f\n",
      part[1], substr(part[2], 1, 26), r, io, rc, rc / r,
      p99[key, "redis"] / n[key, "redis"], p99[key, "redis-io"] / n[key, "redis-io"], p99[key, "recache"] / n[key, "recache"],
      worst[key, "redis"], worst[key, "redis-io"], worst[key, "recache"];
  }
}' "$out"
