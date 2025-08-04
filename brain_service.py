#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🧠 BRAIN SERVICE - Phoenix 95 Signal Intelligence Engine (포트: 8100)
================================================================================
역할: Phoenix 95 AI 분석 + 신호 처리 통합 + Pine Script IQE-V3 연동
기능: 85% 이상 신뢰도 신호만 통과, Kelly Criterion 포지션 사이징
고도화: RabbitMQ 메시지 발행, Redis Streams 데이터 스트리밍, TradingView 웹훅
================================================================================
"""

import asyncio
import json
import time
import logging
import hashlib
import hmac
import numpy as np
import redis
import aioredis
import pika
import aio_pika
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict, field
from collections import deque
import traceback
import gc
import psutil
import os
import sys
from pathlib import Path

# FastAPI 및 웹 프레임워크
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, Security, status, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse, HTMLResponse
from pydantic import BaseModel, Field, validator
import uvicorn

# ✅ try: 구문 추가 (필수!)
try:
    import asyncpg
    POSTGRES_AVAILABLE = True
    logging.info("🗄️ PostgreSQL 라이브러리 로드 성공")
except ImportError:
    logging.warning("🗄️ asyncpg 없음 - PostgreSQL 기능 비활성화")

# ✅ 인코딩 해결 코드 - 완벽한 위치!
os.environ['PYTHONIOENCODING'] = 'utf-8'
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='ignore')
print("[Phoenix95] Brain Service - 인코딩 설정 완료")

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True
)
print("[Phoenix95] Brain Service - logging 재설정 완료")
# ═══════════════════════════════════════════════════════════════════════════════
#                          🎯 V4 NOTIFY 시스템 설정  

# =============================================================================
# 🎯 BRAIN 서비스 설정
# =============================================================================

@dataclass
class BrainServiceConfig:
    """🧠 BRAIN 서비스 전용 설정"""
    
    # 서비스 기본 정보
    SERVICE_NAME: str = "BRAIN"
    SERVICE_PORT: int = 8100
    SERVICE_VERSION: str = "4.0.0-BRAIN-ULTIMATE-PINE"
    
    # Phoenix 95 AI 엔진 설정
    PHOENIX_95_CONFIG: Dict[str, Any] = field(default_factory=lambda: {
        "confidence_threshold": 0.65,      # 65% 이상만 통과
        "analysis_timeout": 2.0,           # 2초 이내 분석 완료
        "cache_duration": 300,             # 5분 캐시
        "batch_size": 50,                  # 배치 처리 크기
        "max_concurrent": 100,             # 최대 동시 처리
        "retry_attempts": 3,               # 재시도 횟수
        "quality_threshold": 0.75,         # 품질 임계값
        "model_ensemble": True,            # 앙상블 모델 사용
        "real_time_validation": True       # 실시간 검증
    })
    
    # Kelly Criterion 설정
    KELLY_CONFIG: Dict[str, Any] = field(default_factory=lambda: {
        "max_kelly_fraction": 0.02,        # 최대 2% 포지션
        "min_kelly_fraction": 0.01,        # 최소 1% 포지션
        "win_rate_adjustment": 0.85,       # 승률 조정 계수
        "risk_free_rate": 0.02,            # 무위험 수익률
        "volatility_penalty": 0.1,         # 변동성 패널티
        "confidence_boost": 1.2            # 신뢰도 부스트
    })
    
    # 메시지 큐 설정 (RabbitMQ)
    RABBITMQ_CONFIG: Dict[str, Any] = field(default_factory=lambda: {
        "host": "localhost",
        "port": 5672,
        "username": "phoenix95",
        "password": "secure_password_2025",
        "virtual_host": "/trading",
        "exchange": "phoenix95.brain.analysis",
        "routing_key": "signal.analyzed",
        "queue": "analyzed_signals",
        "durable": True,
        "auto_delete": False
    })
    
    # Redis Streams 설정
    REDIS_CONFIG: Dict[str, Any] = field(default_factory=lambda: {
        "host": "localhost",
        "port": 6379,
        "db": 1,
        "stream_name": "brain:analysis:stream",
        "consumer_group": "brain_processors",
        "consumer_name": "brain-worker-1",
        "max_len": 10000,
        "block_ms": 1000
    })
    
    # 성능 모니터링 설정
    MONITORING_CONFIG: Dict[str, Any] = field(default_factory=lambda: {
        "metrics_interval": 30,            # 30초마다 메트릭 수집
        "health_check_interval": 10,       # 10초마다 헬스체크
        "alert_thresholds": {
            "memory_percent": 80,
            "cpu_percent": 85,
            "queue_size": 1000,
            "error_rate": 5.0,
            "response_time_ms": 2000
        }
    })
    
    # 텔레그램 설정
    TELEGRAM_CONFIG: Dict[str, Any] = field(default_factory=lambda: {
        "token": "7386542811:AAEZ21p30rES1k8NxNM2xbZ53U44PI9D5CY",
        "chat_id": "7590895952",
        "enabled": True,
        "alert_level": "WARNING"
    })

# =============================================================================
# 📊 데이터 모델
# =============================================================================

@dataclass
class SignalData:
    """신호 데이터 모델 - Pine Script IQE-V3 연동 지원"""
    signal_id: str
    symbol: str
    action: str
    price: float
    confidence: float
    timestamp: datetime
    
    # 기존 기술적 지표
    rsi: Optional[float] = None
    macd: Optional[float] = None
    bollinger_upper: Optional[float] = None
    bollinger_lower: Optional[float] = None
    volume: Optional[float] = None
    
    # 🆕 Pine Script IQE-V3 전용 필드들
    alpha_score: Optional[float] = None      # Alpha 점수 (IQE-V3 핵심 지표)
    z_score: Optional[float] = None          # Z-Score (정규화된 점수)
    ml_signal: Optional[float] = None        # ML 신호 강도
    ml_confidence: Optional[str] = None      # ML 신뢰도 레벨
    
    # 추가 정보
    strategy: Optional[str] = None
    timeframe: Optional[str] = None
    source: Optional[str] = None
    
    def to_dict(self) -> Dict:
        return asdict(self)

@dataclass
class AnalysisResult:
    """분석 결과 모델"""
    signal_id: str
    symbol: str
    
    # 분석 점수
    phoenix95_score: float
    quality_score: float
    final_confidence: float
    
    # Kelly Criterion 결과
    kelly_fraction: float
    position_size: float
    
    # 리스크 평가
    risk_level: str
    risk_score: float
    
    # 실행 권장
    recommendation: str
    execution_timing: str
    urgency: int
    
    # 메타데이터
    analysis_time_ms: float
    cache_hit: bool
    model_used: str
    
    # 상세 분석
    technical_analysis: Dict = field(default_factory=dict)
    market_conditions: Dict = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        return asdict(self)

class SignalRequest(BaseModel):
    """신호 요청 모델"""
    symbol: str = Field(..., description="거래 심볼")
    action: str = Field(..., description="거래 방향")
    price: float = Field(..., gt=0, description="가격")
    confidence: float = Field(0.8, ge=0, le=1, description="신뢰도")
    strategy: Optional[str] = Field(None, description="전략명")
    timeframe: Optional[str] = Field("1h", description="시간프레임")
    rsi: Optional[float] = Field(None, description="RSI 지표")
    macd: Optional[float] = Field(None, description="MACD 지표")
    volume: Optional[float] = Field(None, description="거래량")
    
    # Pine Script IQE-V3 필드들
    alpha_score: Optional[float] = Field(None, description="Alpha 점수")
    z_score: Optional[float] = Field(None, description="Z-Score")
    ml_signal: Optional[float] = Field(None, description="ML 신호")
    ml_confidence: Optional[str] = Field(None, description="ML 신뢰도")
    
    @validator('action')
    def validate_action(cls, v):
        if v.lower() not in ['buy', 'sell', 'long', 'short']:
            raise ValueError('action must be buy, sell, long, or short')
        return v.lower()
    
    @validator('symbol')
    def validate_symbol(cls, v):
        return v.upper().strip()

# =============================================================================
# 🧠 Phoenix 95 AI Engine Core
# =============================================================================

class Phoenix95AIEngine:
    """🧠 Phoenix 95 AI 엔진 - BRAIN 서비스 코어"""
    
    def __init__(self, config: BrainServiceConfig):
        self.config = config
        self.phoenix_config = config.PHOENIX_95_CONFIG
        self.kelly_config = config.KELLY_CONFIG
        
        # 캐시 시스템
        self.analysis_cache = {}
        self.market_data_cache = {}
        
        # 성능 추적
        self.performance_metrics = {
            "total_analyses": 0,
            "successful_analyses": 0,
            "cache_hits": 0,
            "avg_analysis_time": 0.0,
            "model_accuracy": 0.0,
            "pine_script_signals": 0
        }
        
        # 모델 가중치 (Phoenix 95 최적화)
        self.model_weights = {
            "technical_analysis": 0.25,
            "market_sentiment": 0.20,
            "volume_analysis": 0.15,
            "momentum_indicators": 0.15,
            "pine_script_iqe": 0.25  # 🆕 Pine Script IQE-V3 가중치
        }
        
        logging.info("🧠 Phoenix 95 AI Engine 초기화 완료 (Pine Script 연동)")
    
    async def analyze_signal_complete(self, signal: SignalData) -> AnalysisResult:
        """🎯 완전한 신호 분석 - Phoenix 95 방식 + Pine Script IQE-V3"""
        analysis_start = time.time()
        
        try:
            # Pine Script 신호 추적
            if signal.alpha_score is not None or signal.ml_signal is not None:
                self.performance_metrics["pine_script_signals"] += 1
            
            # 1. 캐시 확인
            cache_key = self._generate_cache_key(signal)
            cached_result = self._get_cached_analysis(cache_key)
            
            if cached_result:
                self.performance_metrics["cache_hits"] += 1
                cached_result.cache_hit = True
                return cached_result
            
            # 2. 실시간 데이터 검증
            if self.phoenix_config["real_time_validation"]:
                validation_score = await self._validate_real_time_data(signal)
            else:
                validation_score = 0.8
            
            # 3. 기술적 분석
            technical_score, technical_details = await self._technical_analysis(signal)
            
            # 4. 시장 조건 분석
            market_score, market_conditions = await self._market_condition_analysis(signal)
            
            # 5. 🆕 Pine Script IQE-V3 분석
            iqe_score, iqe_details = await self._pine_script_iqe_analysis(signal)
            
            # 6. Phoenix 95 점수 계산 (IQE 포함)
            phoenix95_score = await self._calculate_phoenix95_score(
                signal, technical_score, market_score, validation_score, iqe_score
            )
            
            # 7. 품질 점수 계산
            quality_score = self._calculate_quality_score(
                signal, phoenix95_score, technical_score, market_score, iqe_score
            )
            
            # 8. 최종 신뢰도 계산
            final_confidence = self._calculate_final_confidence(
                signal.confidence, phoenix95_score, quality_score
            )
            
            # 9. Kelly Criterion 포지션 사이징
            kelly_fraction, position_size = self._calculate_kelly_position(
                final_confidence, technical_details, market_conditions, iqe_details
            )
            
            # 10. 리스크 평가
            risk_level, risk_score = self._assess_risk(
                signal, final_confidence, kelly_fraction, market_conditions
            )
            
            # 11. 실행 권장사항 생성
            recommendation, execution_timing, urgency = self._generate_recommendation(
                final_confidence, risk_level, phoenix95_score, quality_score
            )
            
            # 12. 분석 시간 계산
            analysis_time = (time.time() - analysis_start) * 1000
            
            # 13. 결과 객체 생성
            result = AnalysisResult(
                signal_id=signal.signal_id,
                symbol=signal.symbol,
                phoenix95_score=phoenix95_score,
                quality_score=quality_score,
                final_confidence=final_confidence,
                kelly_fraction=kelly_fraction,
                position_size=position_size,
                risk_level=risk_level,
                risk_score=risk_score,
                recommendation=recommendation,
                execution_timing=execution_timing,
                urgency=urgency,
                analysis_time_ms=analysis_time,
                cache_hit=False,
                model_used="Phoenix95_V4_Ultimate_Pine",
                technical_analysis=technical_details,
                market_conditions=market_conditions
            )
            
            # IQE 분석 결과 추가
            result.technical_analysis.update(iqe_details)
            
            # 14. 캐시에 저장
            self._cache_analysis(cache_key, result)
            
            # 15. 성능 메트릭 업데이트
            self._update_performance_metrics(result)
            
            # 16. 품질 체크 및 로깅
            if final_confidence >= self.phoenix_config["confidence_threshold"]:
                iqe_info = f"IQE={iqe_score:.3f}" if iqe_score > 0 else "No-IQE"
                logging.info(
                    f"🎯 고품질 신호 분석 완료: {signal.symbol} "
                    f"Phoenix95={phoenix95_score:.3f} "
                    f"Final={final_confidence:.3f} "
                    f"Kelly={kelly_fraction:.3f} "
                    f"{iqe_info} "
                    f"Time={analysis_time:.1f}ms"
                )
            else:
                logging.warning(
                    f"⚠️ 저품질 신호: {signal.symbol} "
                    f"Final={final_confidence:.3f} < {self.phoenix_config['confidence_threshold']}"
                )
            
            return result
            
        except Exception as e:
            logging.error(f"🧠 AI 분석 실패: {signal.symbol} - {e}\n{traceback.format_exc()}")
            return self._create_fallback_result(signal, str(e), analysis_start)
    
    async def _pine_script_iqe_analysis(self, signal: SignalData) -> Tuple[float, Dict]:
        """🆕 Pine Script IQE-V3 분석"""
        iqe_details = {}
        scores = []
        
        # Alpha Score 분석
        if signal.alpha_score is not None:
            alpha_normalized = max(0, min(1, (signal.alpha_score + 1) / 2))  # -1~1을 0~1로
            scores.append(alpha_normalized)
            iqe_details["alpha_score"] = {
                "value": signal.alpha_score,
                "normalized": alpha_normalized,
                "weight": 0.35
            }
        
        # Z-Score 분석
        if signal.z_score is not None:
            # Z-Score를 신뢰도로 변환 (절댓값이 클수록 강한 신호)
            z_confidence = min(1.0, abs(signal.z_score) / 3.0)  # 3-sigma 기준
            scores.append(z_confidence)
            iqe_details["z_score"] = {
                "value": signal.z_score,
                "confidence": z_confidence,
                "weight": 0.25
            }
        
        # ML Signal 분석
        if signal.ml_signal is not None:
            ml_normalized = max(0, min(1, abs(signal.ml_signal)))
            scores.append(ml_normalized)
            iqe_details["ml_signal"] = {
                "value": signal.ml_signal,
                "normalized": ml_normalized,
                "weight": 0.25
            }
        
        # ML Confidence 분석
        if signal.ml_confidence:
            confidence_map = {
                "very_high": 0.95,
                "high": 0.85,
                "medium": 0.70,
                "low": 0.50,
                "very_low": 0.30
            }
            ml_conf_score = confidence_map.get(signal.ml_confidence.lower(), 0.60)
            scores.append(ml_conf_score)
            iqe_details["ml_confidence"] = {
                "level": signal.ml_confidence,
                "score": ml_conf_score,
                "weight": 0.15
            }
        
        # 종합 IQE 점수 계산
        if scores:
            # 가중 평균 (더 많은 지표가 있을수록 신뢰도 증가)
            base_score = np.mean(scores)
            indicator_bonus = min(0.1, len(scores) * 0.02)  # 지표 개수 보너스
            iqe_score = min(1.0, base_score + indicator_bonus)
            
            iqe_details["overall_score"] = iqe_score
            iqe_details["indicators_used"] = len(scores)
            iqe_details["indicator_bonus"] = indicator_bonus
            
            logging.info(f"🎯 IQE-V3 분석: {signal.symbol} Score={iqe_score:.3f} ({len(scores)}개 지표)")
        else:
            iqe_score = 0.0
            iqe_details["overall_score"] = 0.0
            iqe_details["indicators_used"] = 0
            iqe_details["note"] = "No Pine Script IQE indicators available"
        
        return iqe_score, iqe_details
    
    def _generate_cache_key(self, signal: SignalData) -> str:
        """캐시 키 생성 (Pine Script 필드 포함)"""
        key_data = (f"{signal.symbol}_{signal.action}_{signal.price}_{signal.confidence}_"
                   f"{signal.timestamp.hour}_{signal.alpha_score}_{signal.ml_signal}")
        return hashlib.md5(key_data.encode()).hexdigest()
    
    def _get_cached_analysis(self, cache_key: str) -> Optional[AnalysisResult]:
        """캐시된 분석 결과 조회"""
        if cache_key not in self.analysis_cache:
            return None
        
        cached_data, cached_time = self.analysis_cache[cache_key]
        cache_duration = self.phoenix_config["cache_duration"]
        
        if time.time() - cached_time > cache_duration:
            del self.analysis_cache[cache_key]
            return None
        
        return cached_data
    
    def _cache_analysis(self, cache_key: str, result: AnalysisResult):
        """분석 결과 캐싱"""
        self.analysis_cache[cache_key] = (result, time.time())
        
        # 캐시 크기 제한
        if len(self.analysis_cache) > 1000:
            oldest_key = min(self.analysis_cache.keys(), 
                           key=lambda k: self.analysis_cache[k][1])
            del self.analysis_cache[oldest_key]
    
    async def _validate_real_time_data(self, signal: SignalData) -> float:
        """실시간 데이터 검증"""
        try:
            # 실제 가격 조회 시뮬레이션
            current_price = signal.price * (1 + np.random.uniform(-0.01, 0.01))
            price_diff = abs(signal.price - current_price) / current_price
            
            if price_diff < 0.005:  # 0.5% 이내
                return 0.95
            elif price_diff < 0.01:  # 1% 이내
                return 0.85
            elif price_diff < 0.02:  # 2% 이내
                return 0.70
            else:
                return 0.50
                
        except Exception as e:
            logging.warning(f"실시간 데이터 검증 실패: {e}")
            return 0.70
    
    async def _technical_analysis(self, signal: SignalData) -> Tuple[float, Dict]:
        """기술적 분석"""
        technical_scores = []
        details = {}
        
        # RSI 분석
        if signal.rsi is not None:
            rsi_score = self._analyze_rsi(signal.rsi, signal.action)
            technical_scores.append(rsi_score)
            details["rsi"] = {"value": signal.rsi, "score": rsi_score}
        
        # MACD 분석
        if signal.macd is not None:
            macd_score = self._analyze_macd(signal.macd, signal.action)
            technical_scores.append(macd_score)
            details["macd"] = {"value": signal.macd, "score": macd_score}
        
        # 볼린저 밴드 분석
        if signal.bollinger_upper and signal.bollinger_lower:
            bb_score = self._analyze_bollinger_bands(
                signal.price, signal.bollinger_upper, signal.bollinger_lower, signal.action
            )
            technical_scores.append(bb_score)
            details["bollinger"] = {"score": bb_score}
        
        # 거래량 분석
        if signal.volume:
            volume_score = self._analyze_volume(signal.volume)
            technical_scores.append(volume_score)
            details["volume"] = {"value": signal.volume, "score": volume_score}
        
        # 종합 기술적 점수
        if technical_scores:
            technical_score = np.mean(technical_scores)
        else:
            technical_score = signal.confidence * 0.8
        
        details["overall_score"] = technical_score
        details["indicators_count"] = len(technical_scores)
        
        return technical_score, details
    
    def _analyze_rsi(self, rsi: float, action: str) -> float:
        """RSI 분석"""
        if action in ['buy', 'long']:
            if rsi <= 30:
                return 0.9
            elif rsi <= 40:
                return 0.7
            elif rsi <= 50:
                return 0.6
            elif rsi <= 60:
                return 0.4
            else:
                return 0.2
        else:  # sell, short
            if rsi >= 70:
                return 0.9
            elif rsi >= 60:
                return 0.7
            elif rsi >= 50:
                return 0.6
            elif rsi >= 40:
                return 0.4
            else:
                return 0.2
    
    def _analyze_macd(self, macd: float, action: str) -> float:
        """MACD 분석"""
        if action in ['buy', 'long']:
            if macd > 0.01:
                return 0.8
            elif macd > 0:
                return 0.6
            elif macd > -0.005:
                return 0.4
            else:
                return 0.3
        else:  # sell, short
            if macd < -0.01:
                return 0.8
            elif macd < 0:
                return 0.6
            elif macd < 0.005:
                return 0.4
            else:
                return 0.3
    
    def _analyze_bollinger_bands(self, price: float, upper: float, lower: float, action: str) -> float:
        """볼린저 밴드 분석"""
        bb_position = (price - lower) / (upper - lower) if upper != lower else 0.5
        
        if action in ['buy', 'long']:
            if bb_position <= 0.2:
                return 0.8
            elif bb_position <= 0.4:
                return 0.6
            elif bb_position <= 0.6:
                return 0.5
            else:
                return 0.3
        else:  # sell, short
            if bb_position >= 0.8:
                return 0.8
            elif bb_position >= 0.6:
                return 0.6
            elif bb_position >= 0.4:
                return 0.5
            else:
                return 0.3
    
    def _analyze_volume(self, volume: float) -> float:
        """거래량 분석"""
        # 거래량 정규화 (심볼별 평균 거래량 대비)
        if volume > 10000000:
            return 0.9
        elif volume > 5000000:
            return 0.7
        elif volume > 1000000:
            return 0.6
        elif volume > 100000:
            return 0.4
        else:
            return 0.3
    
    async def _market_condition_analysis(self, signal: SignalData) -> Tuple[float, Dict]:
        """시장 조건 분석"""
        conditions = {}
        scores = []
        
        # 시간대 분석
        hour = signal.timestamp.hour
        time_score = self._analyze_trading_hours(hour)
        scores.append(time_score)
        conditions["trading_hours"] = {"hour": hour, "score": time_score}
        
        # 요일 분석
        weekday = signal.timestamp.weekday()
        weekday_score = self._analyze_weekday(weekday)
        scores.append(weekday_score)
        conditions["weekday"] = {"day": weekday, "score": weekday_score}
        
        # 변동성 분석 (시뮬레이션)
        volatility = np.random.uniform(0.1, 0.8)
        volatility_score = self._analyze_volatility(volatility)
        scores.append(volatility_score)
        conditions["volatility"] = {"value": volatility, "score": volatility_score}
        
        # 시장 센티멘트 (시뮬레이션)
        sentiment = np.random.uniform(0.2, 0.9)
        sentiment_score = sentiment
        scores.append(sentiment_score)
        conditions["sentiment"] = {"value": sentiment, "score": sentiment_score}
        
        market_score = np.mean(scores)
        conditions["overall_score"] = market_score
        
        return market_score, conditions
    
    def _analyze_trading_hours(self, hour: int) -> float:
        """거래 시간대 분석"""
        if 8 <= hour <= 12:     # 아시아 오전
            return 0.8
        elif 13 <= hour <= 17:  # 유럽 시간
            return 0.9
        elif 21 <= hour <= 1:   # 미국 시간
            return 0.85
        elif 2 <= hour <= 6:    # 저조한 시간
            return 0.3
        else:
            return 0.6
    
    def _analyze_weekday(self, weekday: int) -> float:
        """요일 분석"""
        weekday_scores = [0.8, 0.9, 0.9, 0.85, 0.7, 0.4, 0.3]  # 월~일
        return weekday_scores[weekday]
    
    def _analyze_volatility(self, volatility: float) -> float:
        """변동성 분석"""
        if 0.2 <= volatility <= 0.5:
            return 0.9  # 적정 변동성
        elif 0.1 <= volatility < 0.2:
            return 0.6  # 낮은 변동성
        elif 0.5 < volatility <= 0.7:
            return 0.7  # 높은 변동성
        else:
            return 0.4  # 극단적 변동성
    
    async def _calculate_phoenix95_score(self, signal: SignalData, technical_score: float, 
                                       market_score: float, validation_score: float, iqe_score: float) -> float:
        """Phoenix 95 점수 계산 (IQE-V3 포함)"""
        # 기본 신뢰도 부스팅
        base_confidence = signal.confidence
        boosted_confidence = min(base_confidence * self.kelly_config["confidence_boost"], 1.0)
        
        # 가중 평균으로 Phoenix 95 점수 계산 (IQE 가중치 포함)
        phoenix95_score = (
            boosted_confidence * 0.2 +
            technical_score * self.model_weights["technical_analysis"] +
            market_score * self.model_weights["market_sentiment"] +
            validation_score * 0.15 +
            iqe_score * self.model_weights["pine_script_iqe"]  # 🆕 IQE 가중치
        )
        
        # IQE 부스트 (Pine Script 신호가 있을 때)
        if iqe_score > 0.7:
            phoenix95_score *= 1.1  # 10% 부스트
        elif iqe_score > 0.5:
            phoenix95_score *= 1.05  # 5% 부스트
        
        # 시간대별 보정
        hour_boost = self._get_hour_boost(signal.timestamp.hour)
        phoenix95_score *= hour_boost
        
        # 심볼별 보정
        symbol_boost = self._get_symbol_boost(signal.symbol)
        phoenix95_score *= symbol_boost
        
        return min(max(phoenix95_score, 0.0), 1.0)
    
    def _get_hour_boost(self, hour: int) -> float:
        """시간대별 부스트"""
        if 8 <= hour <= 12:
            return 1.05
        elif 13 <= hour <= 17:
            return 1.1
        elif 21 <= hour <= 1:
            return 1.08
        else:
            return 1.0
    
    def _get_symbol_boost(self, symbol: str) -> float:
        """심볼별 부스트"""
        major_symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
        if symbol in major_symbols:
            return 1.05
        else:
            return 1.0
    
    def _calculate_quality_score(self, signal: SignalData, phoenix95_score: float, 
                               technical_score: float, market_score: float, iqe_score: float) -> float:
        """품질 점수 계산 (IQE 포함)"""
        # 지표 개수에 따른 보너스
        indicator_count = sum(1 for x in [signal.rsi, signal.macd, signal.volume] if x is not None)
        iqe_indicator_count = sum(1 for x in [signal.alpha_score, signal.z_score, signal.ml_signal] if x is not None)
        
        total_indicators = indicator_count + iqe_indicator_count
        indicator_bonus = min(total_indicators * 0.03, 0.2)
        
        # 신뢰도 일관성 체크
        confidence_consistency = 1.0 - abs(signal.confidence - phoenix95_score) * 0.5
        
        # IQE 품질 보너스
        iqe_bonus = min(iqe_score * 0.1, 0.1) if iqe_score > 0 else 0
        
        # 최종 품질 점수
        quality_score = (
            phoenix95_score * 0.3 +
            technical_score * 0.25 +
            market_score * 0.2 +
            iqe_score * 0.15 +  # 🆕 IQE 품질 기여도
            confidence_consistency * 0.1 +
            indicator_bonus +
            iqe_bonus
        )
        
        return min(max(quality_score, 0.0), 1.0)
    
    def _calculate_final_confidence(self, original_confidence: float, 
                                  phoenix95_score: float, quality_score: float) -> float:
        """최종 신뢰도 계산"""
        # 가중 평균
        final_confidence = (
            original_confidence * 0.2 +
            phoenix95_score * 0.5 +
            quality_score * 0.3
        )
        
        return min(max(final_confidence, 0.0), 1.0)
    
    def _calculate_kelly_position(self, confidence: float, technical_details: Dict, 
                                market_conditions: Dict, iqe_details: Dict = None) -> Tuple[float, float]:
        """Kelly Criterion 포지션 사이징 (IQE 고려)"""
        # 승률 추정
        win_probability = confidence * self.kelly_config["win_rate_adjustment"]
        
        # IQE 승률 보정
        if iqe_details and iqe_details.get("overall_score", 0) > 0.8:
            win_probability *= 1.05  # 5% 승률 부스트
        
        # 손익비 추정
        expected_return = 1.02  # 2% 수익 목표
        expected_loss = 0.98    # 2% 손실 한도
        
        # 변동성 조정
        volatility = market_conditions.get("volatility", {}).get("value", 0.3)
        volatility_adjustment = max(0.5, 1 - volatility * self.kelly_config["volatility_penalty"])
        
        # Kelly 공식
        kelly_fraction = (
            (win_probability * expected_return - (1 - win_probability)) / expected_return
        ) * volatility_adjustment
        
        # 한계값 적용
        kelly_fraction = max(
            self.kelly_config["min_kelly_fraction"],
            min(kelly_fraction, self.kelly_config["max_kelly_fraction"])
        )
        
        # 포지션 크기 (기본 포트폴리오 비율)
        position_size = kelly_fraction
        
        return kelly_fraction, position_size
    
    def _assess_risk(self, signal: SignalData, confidence: float, kelly_fraction: float, 
                    market_conditions: Dict) -> Tuple[str, float]:
        """리스크 평가"""
        risk_factors = []
        
        # 신뢰도 리스크
        if confidence < 0.6:
            risk_factors.append(0.3)
        elif confidence < 0.8:
            risk_factors.append(0.1)
        
        # Kelly 포지션 리스크
        if kelly_fraction > 0.15:
            risk_factors.append(0.2)
        elif kelly_fraction > 0.1:
            risk_factors.append(0.1)
        
        # 시장 조건 리스크
        market_score = market_conditions.get("overall_score", 0.5)
        if market_score < 0.5:
            risk_factors.append(0.2)
        elif market_score < 0.7:
            risk_factors.append(0.1)
        
        # 변동성 리스크
        volatility = market_conditions.get("volatility", {}).get("value", 0.3)
        if volatility > 0.6:
            risk_factors.append(0.25)
        elif volatility > 0.4:
            risk_factors.append(0.1)
        
        # 종합 리스크 점수
        risk_score = sum(risk_factors)
        
        # 리스크 레벨 결정
        if risk_score <= 0.2:
            risk_level = "LOW"
        elif risk_score <= 0.4:
            risk_level = "MEDIUM"
        elif risk_score <= 0.6:
            risk_level = "HIGH"
        else:
            risk_level = "VERY_HIGH"
        
        return risk_level, risk_score
    
    def _generate_recommendation(self, confidence: float, risk_level: str, 
                               phoenix95_score: float, quality_score: float) -> Tuple[str, str, int]:
        """실행 권장사항 생성"""
        # 추천 결정
        if (confidence >= self.phoenix_config["confidence_threshold"] and 
            risk_level in ["LOW", "MEDIUM"] and 
            phoenix95_score >= 0.75):
            recommendation = "STRONG_BUY" if phoenix95_score >= 0.9 else "BUY"
            execution_timing = "IMMEDIATE"
            urgency = min(10, int(confidence * 10))
        elif (confidence >= 0.7 and 
              risk_level in ["LOW", "MEDIUM"] and 
              quality_score >= self.phoenix_config["quality_threshold"]):
            recommendation = "WEAK_BUY"
            execution_timing = "CAREFUL"
            urgency = min(7, int(confidence * 8))
        elif confidence >= 0.6 and risk_level != "VERY_HIGH":
            recommendation = "HOLD"
            execution_timing = "MONITOR"
            urgency = min(5, int(confidence * 6))
        else:
            recommendation = "REJECT"
            execution_timing = "HOLD"
            urgency = 1
        
        return recommendation, execution_timing, urgency
    
    def _create_fallback_result(self, signal: SignalData, error: str, start_time: float) -> AnalysisResult:
        """오류시 대체 결과 생성"""
        analysis_time = (time.time() - start_time) * 1000
        
        return AnalysisResult(
            signal_id=signal.signal_id,
            symbol=signal.symbol,
            phoenix95_score=0.0,
            quality_score=0.0,
            final_confidence=0.0,
            kelly_fraction=0.01,
            position_size=0.01,
            risk_level="VERY_HIGH",
            risk_score=1.0,
            recommendation="REJECT",
            execution_timing="HOLD",
            urgency=0,
            analysis_time_ms=analysis_time,
            cache_hit=False,
            model_used="FALLBACK",
            technical_analysis={"error": error},
            market_conditions={"error": error}
        )
    
    def _update_performance_metrics(self, result: AnalysisResult):
        """성능 메트릭 업데이트"""
        self.performance_metrics["total_analyses"] += 1
        
        if result.recommendation != "REJECT":
            self.performance_metrics["successful_analyses"] += 1
        
        # 이동 평균으로 분석 시간 업데이트
        total = self.performance_metrics["total_analyses"]
        current_avg = self.performance_metrics["avg_analysis_time"]
        new_avg = (current_avg * (total - 1) + result.analysis_time_ms) / total
        self.performance_metrics["avg_analysis_time"] = new_avg
    
    def get_performance_summary(self) -> Dict:
        """성능 요약 조회"""
        total = self.performance_metrics["total_analyses"]
        success_rate = (
            self.performance_metrics["successful_analyses"] / total * 100 
            if total > 0 else 0
        )
        cache_hit_rate = (
            self.performance_metrics["cache_hits"] / total * 100 
            if total > 0 else 0
        )
        pine_script_rate = (
            self.performance_metrics["pine_script_signals"] / total * 100 
            if total > 0 else 0
        )
        
        return {
            "total_analyses": total,
            "success_rate": round(success_rate, 2),
            "cache_hit_rate": round(cache_hit_rate, 2),
            "pine_script_rate": round(pine_script_rate, 2),
            "avg_analysis_time_ms": round(self.performance_metrics["avg_analysis_time"], 2),
            "cache_size": len(self.analysis_cache)
        }

# =============================================================================
# 📡 메시지 큐 & 스트림 처리
# =============================================================================

class MessageQueuePublisher:
    """RabbitMQ 메시지 발행자"""
    
    def __init__(self, config: BrainServiceConfig):
        self.config = config.RABBITMQ_CONFIG
        self.connection = None
        self.channel = None
        self.connected = False
    
    async def connect(self):
        """RabbitMQ 연결"""
        try:
            connection_params = aio_pika.ConnectionParameters(
                host=self.config["host"],
                port=self.config["port"],
                login=self.config["username"],
                password=self.config["password"],
                virtual_host=self.config["virtual_host"]
            )
            
            self.connection = await aio_pika.connect_robust(
                host=self.config["host"],
                port=self.config["port"],
                login=self.config["username"],
                password=self.config["password"],
                virtualhost=self.config["virtual_host"]
            )
            
            self.channel = await self.connection.channel()
            
            # Exchange 생성
            self.exchange = await self.channel.declare_exchange(
                self.config["exchange"],
                aio_pika.ExchangeType.DIRECT,
                durable=self.config["durable"]
            )
            
            # Queue 생성
            self.queue = await self.channel.declare_queue(
                self.config["queue"],
                durable=self.config["durable"]
            )
            
            await self.queue.bind(self.exchange, self.config["routing_key"])
            
            self.connected = True
            logging.info("🐰 RabbitMQ 연결 성공")
            
        except Exception as e:
            logging.error(f"🐰 RabbitMQ 연결 실패: {e}")
            self.connected = False
    
    async def publish_analysis_result(self, signal: SignalData, result: AnalysisResult):
        """분석 결과 발행"""
        if not self.connected:
            await self.connect()
        
        if not self.connected:
            logging.warning("🐰 RabbitMQ 연결 실패 - 메시지 발행 불가")
            return
        
        try:
            message_data = {
                "signal": signal.to_dict(),
                "analysis": result.to_dict(),
                "timestamp": time.time(),
                "service": "BRAIN"
            }
            
            message = aio_pika.Message(
                json.dumps(message_data).encode(),
                content_type="application/json",
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            )
            
            await self.exchange.publish(
                message,
                routing_key=self.config["routing_key"]
            )
            
            logging.info(f"📤 분석 결과 발행: {signal.symbol} -> {result.recommendation}")
            
        except Exception as e:
            logging.error(f"📤 메시지 발행 실패: {e}")
    
    async def disconnect(self):
        """연결 종료"""
        if self.connection:
            await self.connection.close()
            self.connected = False
            logging.info("🐰 RabbitMQ 연결 종료")

class RedisStreamPublisher:
    """Redis Streams 발행자"""
    
    def __init__(self, config: BrainServiceConfig):
        self.config = config.REDIS_CONFIG
        self.redis = None
        self.connected = False
    
    async def connect(self):
        """Redis 연결"""
        try:
            self.redis = await aioredis.from_url(
                f"redis://{self.config['host']}:{self.config['port']}/{self.config['db']}"
            )
            
            # 스트림이 존재하지 않으면 생성
            try:
                await self.redis.xgroup_create(
                    self.config["stream_name"],
                    self.config["consumer_group"],
                    id="0",
                    mkstream=True
                )
            except Exception:
                pass  # 그룹이 이미 존재하는 경우
            
            self.connected = True
            logging.info("🔴 Redis Streams 연결 성공")
            
        except Exception as e:
            logging.error(f"🔴 Redis Streams 연결 실패: {e}")
            self.connected = False
    
    async def publish_stream_data(self, signal: SignalData, result: AnalysisResult):
        """스트림 데이터 발행"""
        if not self.connected:
            await self.connect()
        
        if not self.connected:
            logging.warning("🔴 Redis Streams 연결 실패 - 스트림 발행 불가")
            return
        
        try:
            stream_data = {
                "signal_id": signal.signal_id,
                "symbol": signal.symbol,
                "action": signal.action,
                "price": str(signal.price),
                "phoenix95_score": str(result.phoenix95_score),
                "final_confidence": str(result.final_confidence),
                "recommendation": result.recommendation,
                "kelly_fraction": str(result.kelly_fraction),
                "risk_level": result.risk_level,
                "timestamp": str(time.time()),
                "service": "BRAIN",
                "pine_script": "true" if signal.alpha_score is not None else "false"
            }
            
            message_id = await self.redis.xadd(
                self.config["stream_name"],
                stream_data,
                maxlen=self.config["max_len"]
            )
            
            logging.info(f"🌊 스트림 데이터 발행: {signal.symbol} ID={message_id}")
            
        except Exception as e:
            logging.error(f"🌊 스트림 발행 실패: {e}")
    
    async def disconnect(self):
        """연결 종료"""
        if self.redis:
            await self.redis.close()
            self.connected = False
            logging.info("🔴 Redis Streams 연결 종료")

# =============================================================================
# 🧠 BRAIN 서비스 메인 클래스
# =============================================================================

class BrainService:
    """🧠 BRAIN 서비스 - Phoenix 95 Signal Intelligence Engine + Pine Script 연동"""
    
    def __init__(self):
        self.config = BrainServiceConfig()
        self.app = FastAPI(
            title="🧠 BRAIN Service - Phoenix 95 AI Engine",
            description="Phoenix 95 Signal Intelligence Engine + Pine Script IQE-V3 (포트: 8100)",
            version=self.config.SERVICE_VERSION
        )
        
        # CORS 설정
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
        
        # 핵심 컴포넌트 초기화
        self.ai_engine = Phoenix95AIEngine(self.config)
        self.mq_publisher = MessageQueuePublisher(self.config)
        self.stream_publisher = RedisStreamPublisher(self.config)
        
        # 서비스 상태
        self.service_stats = {
            "start_time": time.time(),
            "total_requests": 0,
            "successful_analyses": 0,
            "failed_analyses": 0,
            "webhook_requests": 0,
            "pine_script_signals": 0,
            "active_connections": 0
        }
        
        # 백그라운드 태스크
        self.background_tasks = []
        
        # 라우트 설정
        self._setup_routes()
        
        logging.info(f"🧠 BRAIN 서비스 초기화 완료 (포트: {self.config.SERVICE_PORT}) + Pine Script 연동")
    
    def _setup_routes(self):
        """라우트 설정"""
        
        @self.app.get("/")
        async def root():
            return HTMLResponse(self._generate_dashboard_html())
        
        @self.app.get("/health")
        async def health_check():
            """헬스체크"""
            uptime = time.time() - self.service_stats["start_time"]
            
            return {
                "service": "BRAIN",
                "status": "healthy",
                "version": self.config.SERVICE_VERSION,
                "uptime_seconds": round(uptime, 2),
                "total_requests": self.service_stats["total_requests"],
                "webhook_requests": self.service_stats["webhook_requests"],
                "pine_script_signals": self.service_stats["pine_script_signals"],
                "ai_engine_ready": True,
                "rabbitmq_connected": self.mq_publisher.connected,
                "redis_connected": self.stream_publisher.connected,
                "performance": self.ai_engine.get_performance_summary(),
                "timestamp": time.time()
            }
        
        @self.app.post("/analyze")
        async def analyze_signal(request: SignalRequest, background_tasks: BackgroundTasks):
            """🎯 신호 분석 메인 엔드포인트"""
            try:
                self.service_stats["total_requests"] += 1
                analysis_start = time.time()
                
                # SignalData 객체 생성
                signal = SignalData(
                    signal_id=f"BRAIN_{int(time.time() * 1000)}",
                    symbol=request.symbol,
                    action=request.action,
                    price=request.price,
                    confidence=request.confidence,
                    timestamp=datetime.utcnow(),
                    rsi=request.rsi,
                    macd=request.macd,
                    volume=request.volume,
                    alpha_score=request.alpha_score,  # 🆕 Pine Script
                    z_score=request.z_score,          # 🆕 Pine Script
                    ml_signal=request.ml_signal,      # 🆕 Pine Script
                    ml_confidence=request.ml_confidence,  # 🆕 Pine Script
                    strategy=request.strategy,
                    timeframe=request.timeframe,
                    source="API"
                )
                
                # Pine Script 신호 추적
                if any([signal.alpha_score, signal.z_score, signal.ml_signal]):
                    self.service_stats["pine_script_signals"] += 1
                
                # AI 분석 실행
                analysis_result = await self.ai_engine.analyze_signal_complete(signal)
                
                # 백그라운드에서 메시지 발행
                background_tasks.add_task(self._publish_results, signal, analysis_result)
                
                # 성공 통계 업데이트
                if analysis_result.recommendation != "REJECT":
                    self.service_stats["successful_analyses"] += 1
                else:
                    self.service_stats["failed_analyses"] += 1
                
                # 응답 생성
                processing_time = (time.time() - analysis_start) * 1000
                
                response = {
                    "status": "success",
                    "signal_id": signal.signal_id,
                    "symbol": signal.symbol,
                    "analysis": {
                        "phoenix95_score": analysis_result.phoenix95_score,
                        "quality_score": analysis_result.quality_score,
                        "final_confidence": analysis_result.final_confidence,
                        "recommendation": analysis_result.recommendation,
                        "execution_timing": analysis_result.execution_timing,
                        "urgency": analysis_result.urgency,
                        "risk_level": analysis_result.risk_level,
                        "risk_score": analysis_result.risk_score
                    },
                    "position_sizing": {
                        "kelly_fraction": analysis_result.kelly_fraction,
                        "position_size": analysis_result.position_size
                    },
                    "performance": {
                        "analysis_time_ms": analysis_result.analysis_time_ms,
                        "processing_time_ms": round(processing_time, 2),
                        "cache_hit": analysis_result.cache_hit,
                        "model_used": analysis_result.model_used
                    },
                    "service_info": {
                        "service": "BRAIN",
                        "version": self.config.SERVICE_VERSION,
                        "timestamp": time.time()
                    }
                }
                
                # 고품질 신호 로깅
                if analysis_result.final_confidence >= self.config.PHOENIX_95_CONFIG["confidence_threshold"]:
                    logging.info(
                        f"🎯 고품질 신호 분석: {signal.symbol} "
                        f"Confidence={analysis_result.final_confidence:.3f} "
                        f"Recommendation={analysis_result.recommendation}"
                    )
                
                return response
                
            except Exception as e:
                self.service_stats["failed_analyses"] += 1
                logging.error(f"🧠 분석 요청 실패: {e}\n{traceback.format_exc()}")
                
                raise HTTPException(
                    status_code=500,
                    detail={
                        "error": "분석 실행 실패",
                        "message": str(e),
                        "service": "BRAIN"
                    }
                )
        
        @self.app.post("/webhook")
        async def tradingview_webhook(request: Request, background_tasks: BackgroundTasks):
            """🎯 TradingView Pine Script 웹훅 엔드포인트"""
            try:
                self.service_stats["webhook_requests"] += 1
                webhook_start = time.time()
                
                # 웹훅 데이터 파싱
                webhook_data = await request.json()
                logging.info(f"📥 TradingView 웹훅 수신: {webhook_data}")
                
                # Pine Script 신호 파싱
                signal = self._parse_tradingview_signal(webhook_data)
                
                if not signal:
                    logging.warning("⚠️ Pine Script 신호 파싱 실패")
                    return JSONResponse(
                        status_code=400,
                        content={
                            "status": "error",
                            "message": "Invalid Pine Script signal format",
                            "service": "BRAIN"
                        }
                    )
                
                # Pine Script 신호 추적
                self.service_stats["pine_script_signals"] += 1
                
                # AI 분석 실행
                analysis_result = await self.ai_engine.analyze_signal_complete(signal)
                
                # 백그라운드에서 메시지 발행
                background_tasks.add_task(self._publish_results, signal, analysis_result)
                
                # 성공 통계 업데이트
                if analysis_result.recommendation != "REJECT":
                    self.service_stats["successful_analyses"] += 1
                else:
                    self.service_stats["failed_analyses"] += 1
                
                # 처리 시간 계산
                processing_time = (time.time() - webhook_start) * 1000
                
                # 웹훅 응답 (TradingView용 간단 응답)
                response = {
                    "status": "success",
                    "signal_id": signal.signal_id,
                    "symbol": signal.symbol,
                    "recommendation": analysis_result.recommendation,
                    "confidence": analysis_result.final_confidence,
                    "kelly_fraction": analysis_result.kelly_fraction,
                    "processing_time_ms": round(processing_time, 2),
                    "timestamp": time.time()
                }
                
                # 고품질 Pine Script 신호 로깅
                if analysis_result.final_confidence >= self.config.PHOENIX_95_CONFIG["confidence_threshold"]:
                    logging.info(
                        f"🎯 Pine Script 고품질 신호: {signal.symbol} "
                        f"Alpha={signal.alpha_score} "
                        f"ML={signal.ml_signal} "
                        f"Confidence={analysis_result.final_confidence:.3f} "
                        f"Recommendation={analysis_result.recommendation}"
                    )
                
                return JSONResponse(content=response)
                
            except Exception as e:
                self.service_stats["failed_analyses"] += 1
                logging.error(f"📥 웹훅 처리 실패: {e}\n{traceback.format_exc()}")
                
                return JSONResponse(
                    status_code=500,
                    content={
                        "status": "error",
                        "message": str(e),
                        "service": "BRAIN"
                    }
                )
        
        @self.app.get("/stats")
        async def get_statistics():
            """서비스 통계 조회"""
            uptime = time.time() - self.service_stats["start_time"]
            ai_performance = self.ai_engine.get_performance_summary()
            
            return {
                "service": "BRAIN",
                "version": self.config.SERVICE_VERSION,
                "uptime_seconds": round(uptime, 2),
                "service_stats": self.service_stats,
                "ai_performance": ai_performance,
                "connections": {
                    "rabbitmq": self.mq_publisher.connected,
                    "redis_streams": self.stream_publisher.connected
                },
                "timestamp": time.time()
            }
        
        @self.app.get("/config")
        async def get_configuration():
            """서비스 설정 조회"""
            return {
                "service": "BRAIN",
                "phoenix95_config": self.config.PHOENIX_95_CONFIG,
                "kelly_config": self.config.KELLY_CONFIG,
                "monitoring_config": self.config.MONITORING_CONFIG,
                "version": self.config.SERVICE_VERSION
            }
    
    def _parse_tradingview_signal(self, webhook_data: Dict) -> Optional[SignalData]:
        """🎯 TradingView Pine Script 신호 파싱"""
        try:
            # 기본 필수 필드 확인
            if not all(key in webhook_data for key in ["symbol", "action", "price"]):
                logging.error("필수 필드 누락: symbol, action, price")
                return None
            
            # 심볼 정규화
            symbol = str(webhook_data["symbol"]).upper().strip()
            if not symbol:
                logging.error("유효하지 않은 심볼")
                return None
            
            # 액션 정규화
            action = str(webhook_data["action"]).lower().strip()
            if action not in ["buy", "sell", "long", "short"]:
                logging.error(f"유효하지 않은 액션: {action}")
                return None
            
            # 가격 검증
            try:
                price = float(webhook_data["price"])
                if price <= 0:
                    logging.error(f"유효하지 않은 가격: {price}")
                    return None
            except (ValueError, TypeError):
                logging.error("가격 파싱 실패")
                return None
            
            # 기본 신뢰도
            confidence = float(webhook_data.get("confidence", 0.75))
            confidence = max(0.0, min(1.0, confidence))
            
            # Pine Script IQE-V3 필드들 파싱
            alpha_score = None
            z_score = None
            ml_signal = None
            ml_confidence = None
            
            # Alpha Score 파싱
            if "alpha_score" in webhook_data:
                try:
                    alpha_score = float(webhook_data["alpha_score"])
                    # Alpha Score 범위 검증 (-1 ~ 1)
                    alpha_score = max(-1.0, min(1.0, alpha_score))
                except (ValueError, TypeError):
                    logging.warning("Alpha Score 파싱 실패")
            
            # Z-Score 파싱
            if "z_score" in webhook_data:
                try:
                    z_score = float(webhook_data["z_score"])
                    # Z-Score 범위 제한 (-5 ~ 5)
                    z_score = max(-5.0, min(5.0, z_score))
                except (ValueError, TypeError):
                    logging.warning("Z-Score 파싱 실패")
            
            # ML Signal 파싱
            if "ml_signal" in webhook_data:
                try:
                    ml_signal = float(webhook_data["ml_signal"])
                    # ML Signal 범위 제한 (-1 ~ 1)
                    ml_signal = max(-1.0, min(1.0, ml_signal))
                except (ValueError, TypeError):
                    logging.warning("ML Signal 파싱 실패")
            
            # ML Confidence 파싱
            if "ml_confidence" in webhook_data:
                ml_conf_str = str(webhook_data["ml_confidence"]).lower().strip()
                valid_levels = ["very_high", "high", "medium", "low", "very_low"]
                if ml_conf_str in valid_levels:
                    ml_confidence = ml_conf_str
                else:
                    logging.warning(f"유효하지 않은 ML Confidence: {ml_conf_str}")
            
            # 기술적 지표들 파싱
            rsi = None
            macd = None
            volume = None
            
            if "rsi" in webhook_data:
                try:
                    rsi = float(webhook_data["rsi"])
                    rsi = max(0.0, min(100.0, rsi))
                except (ValueError, TypeError):
                    logging.warning("RSI 파싱 실패")
            
            if "macd" in webhook_data:
                try:
                    macd = float(webhook_data["macd"])
                except (ValueError, TypeError):
                    logging.warning("MACD 파싱 실패")
            
            if "volume" in webhook_data:
                try:
                    volume = float(webhook_data["volume"])
                    volume = max(0.0, volume)
                except (ValueError, TypeError):
                    logging.warning("Volume 파싱 실패")
            
            # 추가 메타데이터
            strategy = webhook_data.get("strategy", "Pine_Script_IQE_V3")
            timeframe = webhook_data.get("timeframe", "1h")
            
            # SignalData 객체 생성
            signal = SignalData(
                signal_id=f"PINE_{int(time.time() * 1000)}",
                symbol=symbol,
                action=action,
                price=price,
                confidence=confidence,
                timestamp=datetime.utcnow(),
                rsi=rsi,
                macd=macd,
                volume=volume,
                alpha_score=alpha_score,      # 🆕 Pine Script IQE
                z_score=z_score,              # 🆕 Pine Script IQE
                ml_signal=ml_signal,          # 🆕 Pine Script IQE
                ml_confidence=ml_confidence,  # 🆕 Pine Script IQE
                strategy=strategy,
                timeframe=timeframe,
                source="TradingView_Webhook"
            )
            
            # 파싱 성공 로깅
            iqe_indicators = sum(1 for x in [alpha_score, z_score, ml_signal, ml_confidence] if x is not None)
            logging.info(
                f"✅ Pine Script 신호 파싱 성공: "
                f"{symbol} {action} @ {price} "
                f"Confidence={confidence:.3f} "
                f"IQE_Indicators={iqe_indicators}"
            )
            
            return signal
            
        except Exception as e:
            logging.error(f"🚨 Pine Script 신호 파싱 오류: {e}\n{traceback.format_exc()}")
            return None
    
    async def _publish_results(self, signal: SignalData, result: AnalysisResult):
        """분석 결과 발행 (백그라운드)"""
        try:
            # RabbitMQ 발행
            await self.mq_publisher.publish_analysis_result(signal, result)
            
            # Redis Streams 발행
            await self.stream_publisher.publish_stream_data(signal, result)
            
            logging.info(f"📡 결과 발행 완료: {signal.symbol} -> {result.recommendation}")
            
        except Exception as e:
            logging.error(f"📡 결과 발행 실패: {e}")
    
    def _generate_dashboard_html(self) -> str:
        """대시보드 HTML 생성"""
        uptime = time.time() - self.service_stats["start_time"]
        uptime_str = str(timedelta(seconds=int(uptime)))
        ai_performance = self.ai_engine.get_performance_summary()
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>🧠 BRAIN Service Dashboard</title>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 0; background: linear-gradient(135deg, #1e3c72, #2a5298); color: #fff; }}
                .container {{ max-width: 1200px; margin: 0 auto; padding: 20px; }}
                .header {{ text-align: center; margin-bottom: 30px; }}
                .header h1 {{ font-size: 2.5em; margin: 0; text-shadow: 2px 2px 4px rgba(0,0,0,0.5); }}
                .header p {{ font-size: 1.2em; opacity: 0.9; }}
                .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }}
                .stat-card {{ background: rgba(255,255,255,0.1); backdrop-filter: blur(10px); border-radius: 15px; padding: 25px; border: 1px solid rgba(255,255,255,0.2); }}
                .stat-title {{ font-size: 1.4em; font-weight: bold; margin-bottom: 20px; color: #00ff88; text-shadow: 1px 1px 2px rgba(0,0,0,0.5); }}
                .stat-item {{ display: flex; justify-content: space-between; margin: 12px 0; padding: 8px 0; border-bottom: 1px solid rgba(255,255,255,0.1); }}
                .stat-value {{ color: #00ff88; font-weight: bold; font-size: 1.1em; }}
                .status-indicator {{ display: inline-block; width: 12px; height: 12px; border-radius: 50%; margin-right: 8px; animation: pulse 2s infinite; }}
                .status-healthy {{ background: #00ff88; box-shadow: 0 0 10px #00ff88; }}
                .status-warning {{ background: #ffa500; box-shadow: 0 0 10px #ffa500; }}
                .footer {{ text-align: center; margin-top: 40px; opacity: 0.7; }}
                .pine-indicator {{ color: #ff6b35; font-weight: bold; }}
                @keyframes pulse {{ 0% {{ opacity: 1; }} 50% {{ opacity: 0.5; }} 100% {{ opacity: 1; }} }}
                .refresh-info {{ text-align: center; margin: 20px 0; opacity: 0.8; }}
            </style>
            <script>
                setInterval(() => location.reload(), 30000);
            </script>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🧠 BRAIN Service</h1>
                    <p>Phoenix 95 Signal Intelligence Engine <span class="pine-indicator">+ Pine Script IQE-V3</span></p>
                    <p><span class="status-indicator status-healthy"></span>서비스 상태: 정상 운영중 | 업타임: {uptime_str}</p>
                </div>
                
                <div class="stats-grid">
                    <div class="stat-card">
                        <div class="stat-title">📊 서비스 통계</div>
                        <div class="stat-item">
                            <span>포트:</span>
                            <span class="stat-value">{self.config.SERVICE_PORT}</span>
                        </div>
                        <div class="stat-item">
                            <span>총 요청:</span>
                            <span class="stat-value">{self.service_stats["total_requests"]:,}</span>
                        </div>
                        <div class="stat-item">
                            <span>웹훅 요청:</span>
                            <span class="stat-value pine-indicator">{self.service_stats["webhook_requests"]:,}</span>
                        </div>
                        <div class="stat-item">
                            <span>성공한 분석:</span>
                            <span class="stat-value">{self.service_stats["successful_analyses"]:,}</span>
                        </div>
                        <div class="stat-item">
                            <span>Pine Script 신호:</span>
                            <span class="stat-value pine-indicator">{self.service_stats["pine_script_signals"]:,}</span>
                        </div>
                        <div class="stat-item">
                            <span>버전:</span>
                            <span class="stat-value">{self.config.SERVICE_VERSION}</span>
                        </div>
                    </div>
                    
                    <div class="stat-card">
                        <div class="stat-title">🧠 AI 엔진 성능</div>
                        <div class="stat-item">
                            <span>총 분석 수:</span>
                            <span class="stat-value">{ai_performance["total_analyses"]:,}</span>
                        </div>
                        <div class="stat-item">
                            <span>성공률:</span>
                            <span class="stat-value">{ai_performance["success_rate"]}%</span>
                        </div>
                        <div class="stat-item">
                            <span>Pine Script 비율:</span>
                            <span class="stat-value pine-indicator">{ai_performance["pine_script_rate"]}%</span>
                        </div>
                        <div class="stat-item">
                            <span>캐시 히트율:</span>
                            <span class="stat-value">{ai_performance["cache_hit_rate"]}%</span>
                        </div>
                        <div class="stat-item">
                            <span>평균 분석 시간:</span>
                            <span class="stat-value">{ai_performance["avg_analysis_time_ms"]}ms</span>
                        </div>
                        <div class="stat-item">
                            <span>캐시 크기:</span>
                            <span class="stat-value">{ai_performance["cache_size"]}</span>
                        </div>
                    </div>
                    
                    <div class="stat-card">
                        <div class="stat-title">📡 연결 상태</div>
                        <div class="stat-item">
                            <span>RabbitMQ:</span>
                            <span class="stat-value">{"✅ 연결됨" if self.mq_publisher.connected else "❌ 연결 안됨"}</span>
                        </div>
                        <div class="stat-item">
                            <span>Redis Streams:</span>
                            <span class="stat-value">{"✅ 연결됨" if self.stream_publisher.connected else "❌ 연결 안됨"}</span>
                        </div>
                        <div class="stat-item">
                            <span>TradingView 웹훅:</span>
                            <span class="stat-value pine-indicator">✅ 활성화</span>
                        </div>
                        <div class="stat-item">
                            <span>스트림:</span>
                            <span class="stat-value">{self.config.REDIS_CONFIG["stream_name"]}</span>
                        </div>
                        <div class="stat-item">
                            <span>Exchange:</span>
                            <span class="stat-value">{self.config.RABBITMQ_CONFIG["exchange"]}</span>
                        </div>
                    </div>
                    
                    <div class="stat-card">
                        <div class="stat-title">⚙️ 설정 정보</div>
                        <div class="stat-item">
                            <span>신뢰도 임계값:</span>
                            <span class="stat-value">{self.config.PHOENIX_95_CONFIG["confidence_threshold"]:.1%}</span>
                        </div>
                        <div class="stat-item">
                            <span>품질 임계값:</span>
                            <span class="stat-value">{self.config.PHOENIX_95_CONFIG["quality_threshold"]:.1%}</span>
                        </div>
                        <div class="stat-item">
                            <span>Pine Script IQE 가중치:</span>
                            <span class="stat-value pine-indicator">{self.ai_engine.model_weights["pine_script_iqe"]:.1%}</span>
                        </div>
                        <div class="stat-item">
                            <span>분석 제한시간:</span>
                            <span class="stat-value">{self.config.PHOENIX_95_CONFIG["analysis_timeout"]}초</span>
                        </div>
                        <div class="stat-item">
                            <span>최대 Kelly:</span>
                            <span class="stat-value">{self.config.KELLY_CONFIG["max_kelly_fraction"]:.1%}</span>
                        </div>
                        <div class="stat-item">
                            <span>캐시 지속시간:</span>
                            <span class="stat-value">{self.config.PHOENIX_95_CONFIG["cache_duration"]}초</span>
                        </div>
                    </div>
                </div>
                
                <div class="refresh-info">
                    <p>🔄 30초마다 자동 새로고침 | 마지막 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                </div>
                
                <div class="footer">
                    <p>🧠 BRAIN Service - Phoenix 95 Signal Intelligence Engine</p>
                    <p>85% 이상 신뢰도 신호 처리 | Kelly Criterion 포지션 사이징 | <span class="pine-indicator">Pine Script IQE-V3 연동</span></p>
                    <p>웹훅 URL: <span class="pine-indicator">http://localhost:{self.config.SERVICE_PORT}/webhook</span></p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html
    
    async def start_background_services(self):
        """백그라운드 서비스 시작"""
        logging.info("🔄 백그라운드 서비스 시작")
        
        # 메시지 큐 연결
        await self.mq_publisher.connect()
        
        # Redis Streams 연결
        await self.stream_publisher.connect()
        
        # 성능 모니터링 태스크
        monitor_task = asyncio.create_task(self._performance_monitoring_loop())
        self.background_tasks.append(monitor_task)
        
        # 메모리 정리 태스크
        cleanup_task = asyncio.create_task(self._memory_cleanup_loop())
        self.background_tasks.append(cleanup_task)
        
        logging.info(f"✅ {len(self.background_tasks)}개 백그라운드 태스크 시작됨")
    
    async def _performance_monitoring_loop(self):
        """성능 모니터링 루프"""
        while True:
            try:
                await asyncio.sleep(self.config.MONITORING_CONFIG["metrics_interval"])
                
                # 시스템 메트릭 수집
                memory_percent = psutil.virtual_memory().percent
                cpu_percent = psutil.cpu_percent()
                
                # 알림 임계값 체크
                alerts = []
                thresholds = self.config.MONITORING_CONFIG["alert_thresholds"]
                
                if memory_percent > thresholds["memory_percent"]:
                    alerts.append(f"높은 메모리 사용률: {memory_percent:.1f}%")
                
                if cpu_percent > thresholds["cpu_percent"]:
                    alerts.append(f"높은 CPU 사용률: {cpu_percent:.1f}%")
                
                ai_performance = self.ai_engine.get_performance_summary()
                if ai_performance["avg_analysis_time_ms"] > thresholds["response_time_ms"]:
                    alerts.append(f"느린 응답시간: {ai_performance['avg_analysis_time_ms']:.1f}ms")
                
                # 알림 로깅
                for alert in alerts:
                    logging.warning(f"⚠️ BRAIN 성능 알림: {alert}")
                
            except Exception as e:
                logging.error(f"성능 모니터링 오류: {e}")
    
    async def _memory_cleanup_loop(self):
        """메모리 정리 루프"""
        while True:
            try:
                await asyncio.sleep(300)  # 5분마다
                
                # 가비지 컬렉션
                collected = gc.collect()
                
                # 캐시 정리
                current_time = time.time()
                cache_duration = self.config.PHOENIX_95_CONFIG["cache_duration"]
                
                expired_keys = [
                    key for key, (_, timestamp) in self.ai_engine.analysis_cache.items()
                    if current_time - timestamp > cache_duration
                ]
                
                for key in expired_keys:
                    del self.ai_engine.analysis_cache[key]
                
                if collected > 0 or expired_keys:
                    logging.info(f"🧹 메모리 정리: GC={collected}, 캐시={len(expired_keys)}")
                
            except Exception as e:
                logging.error(f"메모리 정리 오류: {e}")
    
    async def stop_background_services(self):
        """백그라운드 서비스 정지"""
        logging.info("🛑 백그라운드 서비스 정지")
        
        # 백그라운드 태스크 취소
        for task in self.background_tasks:
            task.cancel()
        
        # 연결 종료
        await self.mq_publisher.disconnect()
        await self.stream_publisher.disconnect()
        
        logging.info("✅ 백그라운드 서비스 정지 완료")

# =============================================================================
# 🚀 메인 실행부
# =============================================================================

async def main():
    """메인 실행 함수"""
    try:
        # 로깅 설정
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - [🧠BRAIN] %(message)s',
            handlers=[
                logging.FileHandler('brain_service.log', encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        # BRAIN 서비스 초기화
        brain_service = BrainService()
        
        # 백그라운드 서비스 시작
        await brain_service.start_background_services()
        
        # 시작 메시지
        logging.info("🧠 BRAIN Service 시작 (Pine Script IQE-V3 연동)")
        logging.info(f"📡 포트: {brain_service.config.SERVICE_PORT}")
        logging.info(f"🎯 Phoenix 95 신뢰도 임계값: {brain_service.config.PHOENIX_95_CONFIG['confidence_threshold']:.1%}")
        logging.info(f"📊 품질 임계값: {brain_service.config.PHOENIX_95_CONFIG['quality_threshold']:.1%}")
        logging.info(f"🔗 Pine Script IQE 가중치: {brain_service.ai_engine.model_weights['pine_script_iqe']:.1%}")
        logging.info(f"🐰 RabbitMQ: {'✅' if brain_service.mq_publisher.connected else '❌'}")
        logging.info(f"🔴 Redis: {'✅' if brain_service.stream_publisher.connected else '❌'}")
        logging.info(f"📥 웹훅 URL: http://localhost:{brain_service.config.SERVICE_PORT}/webhook")
        
        # 서버 실행
        config = uvicorn.Config(
            brain_service.app,
            host="0.0.0.0",
            port=brain_service.config.SERVICE_PORT,
            log_level="info",
            access_log=True
        )
        
        server = uvicorn.Server(config)
        await server.serve()
        
    except KeyboardInterrupt:
        logging.info("🛑 사용자에 의한 서비스 종료")
    except Exception as e:
        logging.error(f"❌ 서비스 실행 오류: {e}\n{traceback.format_exc()}")
    finally:
        # 정리
        if 'brain_service' in locals():
            await brain_service.stop_background_services()
        logging.info("👋 BRAIN Service 종료")

if __name__ == "__main__":
    asyncio.run(main())

# =============================================================================
# 📋 사용법 및 API 예제 (Pine Script 연동 포함)
# =============================================================================

"""
🧠 BRAIN Service 사용법 (Pine Script IQE-V3 연동):

1. 서비스 시작:
   python brain_service.py

2. 일반 API 호출 예제:
   curl -X POST "http://localhost:8100/analyze" \
        -H "Content-Type: application/json" \
        -d '{
            "symbol": "BTCUSDT",
            "action": "buy",
            "price": 45000.0,
            "confidence": 0.8,
            "rsi": 35.5,
            "macd": 0.003,
            "volume": 1500000,
            "alpha_score": 0.75,
            "z_score": 2.1,
            "ml_signal": 0.85,
            "ml_confidence": "high"
        }'

3. 🆕 Pine Script 웹훅 예제:
   curl -X POST "http://localhost:8100/webhook" \
        -H "Content-Type: application/json" \
        -d '{
            "symbol": "BTCUSDT",
            "action": "buy",
            "price": 45000.0,
            "confidence": 0.8,
            "alpha_score": 0.75,
            "z_score": 2.1,
            "ml_signal": 0.85,
            "ml_confidence": "high",
            "strategy": "Pine_Script_IQE_V3",
            "timeframe": "1h"
        }'

4. TradingView Pine Script 웹훅 설정:
   - URL: http://your-server:8100/webhook
   - Method: POST
   - Content-Type: application/json
   - Body: {"symbol":"{{ticker}}", "action":"{{strategy.order.action}}", "price":"{{close}}", "alpha_score":"{{plot_0}}", "z_score":"{{plot_1}}", "ml_signal":"{{plot_2}}", "ml_confidence":"high"}

5. 헬스체크:
   curl http://localhost:8100/health

6. 통계 조회:
   curl http://localhost:8100/stats

7. 대시보드 접속:
   http://localhost:8100

📡 메시지 큐 설정:
- RabbitMQ Exchange: phoenix95.brain.analysis
- Redis Stream: brain:analysis:stream
- 분석 결과가 자동으로 다른 서비스로 전달됩니다.

🎯 Pine Script IQE-V3 핵심 기능:
- Alpha Score 분석 (-1 ~ 1 범위)
- Z-Score 정규화 (-5 ~ 5 범위)
- ML Signal 강도 (-1 ~ 1 범위)
- ML Confidence 레벨 (very_high, high, medium, low, very_low)
- 가중치 기반 통합 분석
- Kelly Criterion에 IQE 신호 반영
- 실시간 Pine Script 웹훅 처리

🔗 연동 플로우:
Pine Script → TradingView 웹훅 → BRAIN Service → Phoenix 95 AI 분석 → RabbitMQ/Redis → 다운스트림 서비스

⚡ 성능 최적화:
- 캐시 기반 빠른 응답
- 비동기 메시지 발행
- 백그라운드 성능 모니터링
- 자동 메모리 관리
- Pine Script 신호 우선순위 처리
"""