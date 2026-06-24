#!/bin/bash

# 🔴 실거래 실행 스크립트
# 
# ⚠️  경고: 실제 돈이 사용됩니다!

echo ""
echo "⚠️  ═══════════════════════════════════════"
echo "⚠️  실거래 시스템 실행 준비"
echo "⚠️  ═══════════════════════════════════════"
echo ""

# 1. 환경 확인
echo "🔍 1단계: 환경 확인 중..."
if [ ! -f ".env" ]; then
    echo "❌ .env 파일이 없습니다!"
    echo "   KIS_GUIDE.md를 참고하여 .env 파일을 생성하세요."
    exit 1
fi

echo "✅ .env 파일 확인 완료"

# 2. API 키 확인
echo ""
echo "🔍 2단계: API 키 확인 중..."
source .env

if [ -z "$KIS_APP_KEY" ] || [ -z "$KIS_APP_SECRET" ]; then
    echo "❌ API 키가 설정되지 않았습니다!"
    echo "   .env 파일에 KIS_APP_KEY와 KIS_APP_SECRET을 설정하세요."
    exit 1
fi

echo "✅ API 키 확인 완료"
echo "   Base URL: $KIS_BASE_URL"

# 3. 실전투자 확인
echo ""
echo "🔍 3단계: 실전투자 모드 확인 중..."
if [[ "$KIS_BASE_URL" == *"openapi.koreainvestment.com:9443"* ]]; then
    echo "⚠️  ⚠️  ⚠️  실전투자 모드입니다! ⚠️  ⚠️  ⚠️"
    echo "    실제 주문이 체결됩니다!"
    echo "    실제 돈이 사용됩니다!"
else
    echo "✅ 모의투자 모드입니다"
fi

# 4. 최종 확인
echo ""
echo "═══════════════════════════════════════════"
echo "실거래 시스템을 시작하시겠습니까?"
echo "═══════════════════════════════════════════"
echo ""
echo "선택:"
echo "  1) 계좌 잔고 확인"
echo "  2) 실거래 시작 (자동매매)"
echo "  3) 취소"
echo ""
read -p "선택 [1-3]: " choice

case $choice in
    1)
        echo ""
        echo "💰 계좌 잔고 확인 중..."
        echo ""
        cargo run --bin kis-balance-check
        ;;
    2)
        echo ""
        echo "🤖 실거래 시작..."
        echo "   중지: Ctrl+C"
        echo ""
        cargo run --bin kis-auto-trading -- --real
        ;;
    3)
        echo ""
        echo "✅ 취소되었습니다"
        exit 0
        ;;
    *)
        echo ""
        echo "❌ 잘못된 선택입니다"
        exit 1
        ;;
esac

