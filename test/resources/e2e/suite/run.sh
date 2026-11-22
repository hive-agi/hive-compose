#!/bin/sh
# orders-suite: a deliberately half-failing e2e suite, in plain POSIX sh + curl.
#
# Protocols only. In: ORDERS_URL, BILLING_URL, HIVE_RUN_ID, TRACEPARENT,
# BAGGAGE (W3C env carriers, as the OTel spec names them), REPORT_FORMAT
# (junit | tap | both, default junit), REPORT_DIR (default /reports).
# Out: JUnit XML (junit.xml) and/or TAP (results.tap) in REPORT_DIR, and the
# exit code: 0 when every case passed, 1 when any failed.
#
# Every request carries a fresh `traceparent` (the trace id of TRACEPARENT when
# given, so the whole suite is one trace) and `baggage` with test.id=<case id>,
# so the services stamp the case onto their spans and `why test=<id>` finds them.
#
# Cases:
#   orders.place_good_order  POST a valid order, expect 202, then expect billing
#                            to invoice it within INVOICE_WAIT_S seconds. PASSES.
#   orders.place_boom_order  POST sku "boom", expect 202. The service rejects it
#                            with 500 "sku boom rejected": FAILS ON PURPOSE.
set -u

ORDERS_URL="${ORDERS_URL:-http://orders:8080}"
BILLING_URL="${BILLING_URL:-http://billing:8080}"
REPORT_FORMAT="${REPORT_FORMAT:-junit}"
REPORT_DIR="${REPORT_DIR:-/reports}"
INVOICE_WAIT_S="${INVOICE_WAIT_S:-30}"
SUITE="orders-suite"
RUN_ID="${HIVE_RUN_ID:-}"

hex() { od -An -N"$1" -tx1 /dev/urandom | tr -d ' \n'; }
now_ms() { echo $(( $(date +%s) * 1000 )); }

# Trace id: inherit TRACEPARENT's (00-<trace>-<span>-<flags>) or mint one.
TRACE_ID=""
case "${TRACEPARENT:-}" in
  00-????????????????????????????????-????????????????-??)
    TRACE_ID=$(echo "$TRACEPARENT" | cut -d- -f2) ;;
esac
[ -n "$TRACE_ID" ] || TRACE_ID=$(hex 16)

mkdir -p "$REPORT_DIR"
WORK=$(mktemp -d)
CASES=0
FAILS=0
START_S=$(date +%s)

# baggage for a case: the inherited BAGGAGE members, test.id, hive.run.id.
baggage_for() {
  b="test.id=$1"
  [ -n "$RUN_ID" ] && b="$b,hive.run.id=$RUN_ID"
  [ -n "${BAGGAGE:-}" ] && b="${BAGGAGE},$b"
  echo "$b"
}

# record <case> <status pass|fail> <seconds> <span-id> <message>
record() {
  CASES=$((CASES + 1))
  [ "$2" = fail ] && FAILS=$((FAILS + 1))
  printf '%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" "$5" >> "$WORK/cases.tsv"
}

# post_order <case-id> <span-id> <json> -> prints the HTTP status, body in $WORK/<case>.body
post_order() {
  curl -sS -o "$WORK/$1.body" -w '%{http_code}' --max-time 10 \
    -H 'Content-Type: application/json' \
    -H "traceparent: 00-$TRACE_ID-$2-01" \
    -H "baggage: $(baggage_for "$1")" \
    -X POST --data "$3" "$ORDERS_URL/orders" 2>"$WORK/$1.err" || echo 000
}

case_good() {
  id="orders.place_good_order"; span=$(hex 8); t0=$(date +%s)
  order_id="good-$(hex 4)"
  code=$(post_order "$id" "$span" "{\"id\":\"$order_id\",\"sku\":\"widget\",\"qty\":2}")
  if [ "$code" != 202 ]; then
    record "$id" fail $(( $(date +%s) - t0 )) "$span" "POST /orders returned $code, expected 202"
    return
  fi
  i=0
  while [ "$i" -lt "$INVOICE_WAIT_S" ]; do
    if curl -sS --max-time 5 -H "traceparent: 00-$TRACE_ID-$(hex 8)-01" \
         -H "baggage: $(baggage_for "$id")" "$BILLING_URL/invoices" 2>/dev/null \
       | grep -q "\"$order_id\""; then
      record "$id" pass $(( $(date +%s) - t0 )) "$span" ""
      return
    fi
    i=$((i + 1)); sleep 1
  done
  record "$id" fail $(( $(date +%s) - t0 )) "$span" "order $order_id was not invoiced by billing within ${INVOICE_WAIT_S}s"
}

case_boom() {
  id="orders.place_boom_order"; span=$(hex 8); t0=$(date +%s)
  code=$(post_order "$id" "$span" "{\"id\":\"boom-$(hex 4)\",\"sku\":\"boom\",\"qty\":1}")
  if [ "$code" = 202 ]; then
    record "$id" pass $(( $(date +%s) - t0 )) "$span" ""
  else
    body=$(tr -d '\n' < "$WORK/$id.body" 2>/dev/null)
    record "$id" fail $(( $(date +%s) - t0 )) "$span" "POST /orders returned $code, expected 202: $body"
  fi
}

xml_escape() { sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g' -e 's/"/\&quot;/g'; }

write_junit() {
  out="$REPORT_DIR/junit.xml"; total_s=$(( $(date +%s) - START_S ))
  ts=$(date -u -d "@$START_S" +%Y-%m-%dT%H:%M:%S 2>/dev/null || date -u +%Y-%m-%dT%H:%M:%S)
  {
    echo '<?xml version="1.0" encoding="UTF-8"?>'
    echo "<testsuites name=\"$SUITE\" tests=\"$CASES\" failures=\"$FAILS\" errors=\"0\" time=\"$total_s\">"
    echo "  <testsuite name=\"$SUITE\" tests=\"$CASES\" failures=\"$FAILS\" errors=\"0\" skipped=\"0\" time=\"$total_s\" timestamp=\"$ts\">"
    echo "    <properties>"
    echo "      <property name=\"trace.id\" value=\"$TRACE_ID\"/>"
    echo "      <property name=\"hive.run.id\" value=\"$(echo "$RUN_ID" | xml_escape)\"/>"
    echo "    </properties>"
    while IFS="$(printf '\t')" read -r id status secs span msg; do
      cls=${id%.*}; name=${id##*.}
      echo "    <testcase classname=\"$cls\" name=\"$name\" time=\"$secs\">"
      echo "      <properties>"
      echo "        <property name=\"test.id\" value=\"$id\"/>"
      echo "        <property name=\"traceparent\" value=\"00-$TRACE_ID-$span-01\"/>"
      echo "      </properties>"
      if [ "$status" = fail ]; then
        m=$(echo "$msg" | xml_escape)
        echo "      <failure message=\"$m\" type=\"AssertionError\">$m</failure>"
      fi
      echo "    </testcase>"
    done < "$WORK/cases.tsv"
    echo "  </testsuite>"
    echo "</testsuites>"
  } > "$out"
}

write_tap() {
  out="$REPORT_DIR/results.tap"; n=0
  {
    echo "TAP version 13"
    echo "1..$CASES"
    while IFS="$(printf '\t')" read -r id status secs span msg; do
      n=$((n + 1))
      if [ "$status" = pass ]; then echo "ok $n - $id"; else echo "not ok $n - $id"; fi
      echo "  ---"
      echo "  test.id: $id"
      echo "  traceparent: 00-$TRACE_ID-$span-01"
      echo "  duration_s: $secs"
      [ "$status" = fail ] && echo "  message: \"$(echo "$msg" | sed 's/"/\\"/g')\""
      echo "  ..."
    done < "$WORK/cases.tsv"
  } > "$out"
}

: > "$WORK/cases.tsv"
case_good
case_boom

case "$REPORT_FORMAT" in
  junit) write_junit ;;
  tap)   write_tap ;;
  both)  write_junit; write_tap ;;
  *)     echo "unknown REPORT_FORMAT=$REPORT_FORMAT (junit|tap|both)" >&2; write_junit ;;
esac

echo "$SUITE: $CASES cases, $FAILS failed, trace $TRACE_ID, reports in $REPORT_DIR"
rm -rf "$WORK"
[ "$FAILS" -eq 0 ]
