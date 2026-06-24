#!/bin/bash
# 매매 알림 스크립트
#
# 사용법:
#   ./notify.sh "제목" "메시지"
#   ./notify.sh "매수 알림" "삼성전자 1주 매수 @ 181,200원"

TITLE="${1:-알림}"
MESSAGE="${2:-메시지 없음}"

# iMessage 전송 (imsg CLI)
send_imessage() {
    if command -v imsg &> /dev/null; then
        echo "📱 iMessage 전송..."
        echo "$MESSAGE" | imsg send -t "+821038600983"
    fi
}

# Telegram 전송 (Clawdbot 연동)
send_telegram() {
    # Clawdbot이 실행 중이면 Telegram으로 전송
    if curl -s http://localhost:18789/health > /dev/null 2>&1; then
        echo "📨 Telegram 전송..."
        curl -s -X POST http://localhost:18789/api/message \
            -H "Content-Type: application/json" \
            -d "{\"channel\": \"telegram\", \"message\": \"$TITLE\n\n$MESSAGE\"}" \
            > /dev/null 2>&1
    fi
}

# macOS 알림
send_macos() {
    osascript -e "display notification \"$MESSAGE\" with title \"$TITLE\"" 2>/dev/null
}

# 알림 전송
echo "🔔 알림 전송: $TITLE"
echo "   $MESSAGE"

send_macos
send_imessage
# send_telegram  # 필요시 활성화

echo "✅ 알림 완료"
