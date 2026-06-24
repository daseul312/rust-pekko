#!/bin/bash
# 장 시작 자동매매 (09:00 실행)
#
# ⚠️ LIVE 모드 - 실제 매매 실행!
# 
# crontab: 0 9 * * 1-5 /path/to/morning-trade.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/morning-trade-$(date +%Y-%m-%d).log"
NOTIFY_SCRIPT="$SCRIPT_DIR/notify.sh"

# 설정
LIVE_TRADING="true"        # 실거래 모드
MAX_AMOUNT="50000"         # 최대 5만원
SYMBOL="005930"            # 삼성전자

mkdir -p "$LOG_DIR"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

notify() {
    if [ -f "$NOTIFY_SCRIPT" ]; then
        "$NOTIFY_SCRIPT" "$1" "$2" >> "$LOG_FILE" 2>&1 || true
    fi
}

# 주말 체크
DAY=$(date +%u)
if [ "$DAY" -gt 5 ]; then
    log "📅 주말 - 매매 중단"
    exit 0
fi

log "═══════════════════════════════════════════════════════"
log "🌅 장 시작 자동매매"
log "   모드: LIVE 🔴"
log "   최대금액: ${MAX_AMOUNT}원"
log "   종목: $SYMBOL"
log "═══════════════════════════════════════════════════════"

# 시작 알림
notify "🌅 장 시작 매매" "자동매매 시작 - $SYMBOL (최대 ${MAX_AMOUNT}원)"

cd "$PROJECT_DIR"

# 트레이딩 실행
log "📊 매매 분석 시작..."

OUTPUT=$(LIVE_TRADING="$LIVE_TRADING" MAX_AMOUNT="$MAX_AMOUNT" SYMBOL="$SYMBOL" \
    cargo run --release -q --example live_trading_test 2>&1)

echo "$OUTPUT" >> "$LOG_FILE"

# 결과 파싱 및 알림
if echo "$OUTPUT" | grep -q "BUY.*체결"; then
    PRICE=$(echo "$OUTPUT" | grep -o "[0-9,]*원" | head -1)
    notify "🟢 매수 체결!" "[$SYMBOL] 매수 완료 @ $PRICE"
    log "✅ 매수 체결됨"
elif echo "$OUTPUT" | grep -q "SELL.*체결"; then
    PRICE=$(echo "$OUTPUT" | grep -o "[0-9,]*원" | head -1)
    notify "🔴 매도 체결!" "[$SYMBOL] 매도 완료 @ $PRICE"
    log "✅ 매도 체결됨"
elif echo "$OUTPUT" | grep -q "Hold\|HOLD"; then
    notify "⏸️ 관망" "[$SYMBOL] 매매 신호 없음 - 관망"
    log "⏸️ 관망 유지"
else
    notify "ℹ️ 매매 완료" "[$SYMBOL] 처리 완료"
    log "ℹ️ 처리 완료"
fi

log ""
log "✨ 장 시작 매매 완료"
