#!/bin/bash
# 자동매매 스케줄러
#
# 장 시간(09:00~15:30)에 30분 간격으로 실행
# crontab: */30 9-15 * * 1-5 /path/to/auto-trade.sh
#
# 환경변수:
# - LIVE_TRADING=true : 실거래 모드 (기본: false)
# - MAX_AMOUNT=100000 : 최대 주문 금액
# - SYMBOLS="005930 000660" : 감시 종목 (기본: 삼성전자)
# - NOTIFY=true : 알림 전송 (기본: true)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"
LOG_FILE="$LOG_DIR/auto-trade-$(date +%Y-%m-%d).log"

# 로그 디렉토리 생성
mkdir -p "$LOG_DIR"

# 기본값 설정
LIVE_TRADING="${LIVE_TRADING:-false}"
MAX_AMOUNT="${MAX_AMOUNT:-100000}"
SYMBOLS="${SYMBOLS:-005930}"
NOTIFY="${NOTIFY:-true}"
USE_LLM="${USE_LLM:-true}"  # LLM 분석 기본 활성화

# 현재 시간 확인
HOUR=$(date +%H)
MINUTE=$(date +%M)
DAY=$(date +%u)  # 1=월, 7=일

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

send_notification() {
    local title="$1"
    local message="$2"
    
    if [ "$NOTIFY" = "true" ]; then
        # notify.sh 스크립트 사용
        if [ -f "$SCRIPT_DIR/notify.sh" ]; then
            "$SCRIPT_DIR/notify.sh" "$title" "$message" >> "$LOG_FILE" 2>&1
        else
            # 폴백: 직접 전송
            if command -v imsg &> /dev/null; then
                echo "$message" | imsg send -t "+821038600983" 2>/dev/null || true
            fi
            osascript -e "display notification \"$message\" with title \"$title\"" 2>/dev/null || true
        fi
    fi
}

# 주말 체크
if [ "$DAY" -gt 5 ]; then
    log "📅 주말 - 매매 중단"
    exit 0
fi

# 장 시간 체크 (09:00 ~ 15:30)
if [ "$HOUR" -lt 9 ] || [ "$HOUR" -gt 15 ]; then
    log "🕐 장외 시간 - 매매 중단"
    exit 0
fi

if [ "$HOUR" -eq 15 ] && [ "$MINUTE" -gt 30 ]; then
    log "🕐 장 마감 - 매매 중단"
    exit 0
fi

log "═══════════════════════════════════════════════════════"
log "🤖 자동매매 시작"
log "   모드: $([ "$LIVE_TRADING" = "true" ] && echo "LIVE 🔴" || echo "DRY-RUN 🧪")"
log "   분석: $([ "$USE_LLM" = "true" ] && echo "LLM (Grok) 🤖" || echo "키워드 📰")"
log "   최대금액: ${MAX_AMOUNT}원"
log "   종목: $SYMBOLS"
log "═══════════════════════════════════════════════════════"

cd "$PROJECT_DIR"

# 각 종목에 대해 실행
for SYMBOL in $SYMBOLS; do
    log ""
    log "📊 종목 분석: $SYMBOL"
    
    # 트레이딩 실행
    OUTPUT=$(LIVE_TRADING="$LIVE_TRADING" MAX_AMOUNT="$MAX_AMOUNT" SYMBOL="$SYMBOL" USE_LLM="$USE_LLM" \
        cargo run --release -q --example live_trading_test 2>&1)
    
    echo "$OUTPUT" >> "$LOG_FILE"
    
    # 결과 파싱
    if echo "$OUTPUT" | grep -q "BUY.*체결"; then
        RESULT="🟢 매수 체결"
        send_notification "자동매매 알림" "[$SYMBOL] $RESULT"
    elif echo "$OUTPUT" | grep -q "SELL.*체결"; then
        RESULT="🔴 매도 체결"
        send_notification "자동매매 알림" "[$SYMBOL] $RESULT"
    elif echo "$OUTPUT" | grep -q "HOLD"; then
        RESULT="⚪ 관망"
    else
        RESULT="⏸️ 신호 없음"
    fi
    
    log "   결과: $RESULT"
    
    # API 호출 간격
    sleep 1
done

log ""
log "✨ 자동매매 완료"
log ""

# 오래된 로그 정리 (7일 이상)
find "$LOG_DIR" -name "auto-trade-*.log" -mtime +7 -delete 2>/dev/null || true
