#!/usr/bin/env python3
"""
Sistema Otimizado de Análise de Potencial de Baús
Otimização para resolver gargalos de performance no Sistema Fature
"""
import asyncio
import logging
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
from enum import Enum
import hashlib
import redis
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ChestType(Enum):
    """Tipos de baús disponíveis"""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    PLATINUM = "platinum"
    DIAMOND = "diamond"

class PotentialLevel(Enum):
    """Níveis de potencial"""
    VERY_LOW = "very_low"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    VERY_HIGH = "very_high"

@dataclass
class UserProfile:
    """Perfil do usuário para análise"""
    user_id: str
    registration_date: datetime
    total_deposits: float
    total_bets: int
    total_ggr: float
    avg_bet_value: float
    last_activity: datetime
    activity_frequency: int  # dias ativos nos últimos 30 dias
    preferred_games: List[str]
    deposit_frequency: int  # depósitos nos últimos 30 dias
    withdrawal_frequency: int  # saques nos últimos 30 dias
    bonus_usage_rate: float  # % de bônus utilizados
    session_duration_avg: float  # minutos por sessão
    device_types: List[str]  # mobile, desktop, tablet
    geographic_region: str
    risk_score: float  # 0-1, onde 1 é alto risco

@dataclass
class ChestConfig:
    """Configuração de um tipo de baú"""
    chest_type: ChestType
    min_potential_score: float
    max_potential_score: float
    rewards: Dict[str, Any]
    rarity_weights: Dict[str, float]
    cooldown_hours: int
    max_per_day: int
    cost_multiplier: float

@dataclass
class PotentialAnalysis:
    """Resultado da análise de potencial"""
    user_id: str
    potential_level: PotentialLevel
    potential_score: float  # 0-100
    recommended_chest: ChestType
    confidence: float  # 0-1
    factors: Dict[str, float]
    analysis_timestamp: datetime
    cache_ttl: int  # segundos

class OptimizedChestAnalyzer:
    """Analisador otimizado de potencial de baús"""
    
    def __init__(self, redis_client=None, config_service_url: str = None):
        self.redis_client = redis_client or redis.Redis(
            host='redis.fature.svc.cluster.local',
            port=6379,
            decode_responses=True
        )
        self.config_service_url = config_service_url or "http://config-service.fature.svc.cluster.local"
        
        # Cache de configurações
        self._chest_configs = {}
        self._analysis_weights = {}
        self._cache_ttl = 300  # 5 minutos
        
        # Pool de threads para processamento paralelo
        self.thread_pool = ThreadPoolExecutor(max_workers=10)
        
        # Métricas de performance
        self.metrics = {
            'analyses_performed': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'avg_analysis_time': 0.0,
            'total_analysis_time': 0.0
        }
    
    def _load_chest_configs(self) -> Dict[ChestType, ChestConfig]:
        """Carrega configurações dos baús do config-service"""
        try:
            # Tenta buscar do cache Redis primeiro
            cached_configs = self.redis_client.get("chest_configs")
            if cached_configs:
                configs_data = json.loads(cached_configs)
                return {
                    ChestType(k): ChestConfig(**v) 
                    for k, v in configs_data.items()
                }
            
            # Configurações padrão otimizadas
            default_configs = {
                ChestType.BRONZE: ChestConfig(
                    chest_type=ChestType.BRONZE,
                    min_potential_score=0.0,
                    max_potential_score=20.0,
                    rewards={
                        "coins": {"min": 10, "max": 50},
                        "bonus_multiplier": {"min": 1.1, "max": 1.3},
                        "free_spins": {"min": 5, "max": 15}
                    },
                    rarity_weights={"common": 0.7, "rare": 0.25, "epic": 0.05},
                    cooldown_hours=1,
                    max_per_day=10,
                    cost_multiplier=1.0
                ),
                ChestType.SILVER: ChestConfig(
                    chest_type=ChestType.SILVER,
                    min_potential_score=20.0,
                    max_potential_score=40.0,
                    rewards={
                        "coins": {"min": 50, "max": 150},
                        "bonus_multiplier": {"min": 1.3, "max": 1.6},
                        "free_spins": {"min": 15, "max": 30}
                    },
                    rarity_weights={"common": 0.5, "rare": 0.4, "epic": 0.1},
                    cooldown_hours=2,
                    max_per_day=8,
                    cost_multiplier=1.5
                ),
                ChestType.GOLD: ChestConfig(
                    chest_type=ChestType.GOLD,
                    min_potential_score=40.0,
                    max_potential_score=65.0,
                    rewards={
                        "coins": {"min": 150, "max": 400},
                        "bonus_multiplier": {"min": 1.6, "max": 2.0},
                        "free_spins": {"min": 30, "max": 60}
                    },
                    rarity_weights={"common": 0.3, "rare": 0.5, "epic": 0.2},
                    cooldown_hours=4,
                    max_per_day=5,
                    cost_multiplier=2.5
                ),
                ChestType.PLATINUM: ChestConfig(
                    chest_type=ChestType.PLATINUM,
                    min_potential_score=65.0,
                    max_potential_score=85.0,
                    rewards={
                        "coins": {"min": 400, "max": 1000},
                        "bonus_multiplier": {"min": 2.0, "max": 3.0},
                        "free_spins": {"min": 60, "max": 120}
                    },
                    rarity_weights={"common": 0.2, "rare": 0.5, "epic": 0.25, "legendary": 0.05},
                    cooldown_hours=8,
                    max_per_day=3,
                    cost_multiplier=4.0
                ),
                ChestType.DIAMOND: ChestConfig(
                    chest_type=ChestType.DIAMOND,
                    min_potential_score=85.0,
                    max_potential_score=100.0,
                    rewards={
                        "coins": {"min": 1000, "max": 5000},
                        "bonus_multiplier": {"min": 3.0, "max": 5.0},
                        "free_spins": {"min": 120, "max": 300}
                    },
                    rarity_weights={"rare": 0.3, "epic": 0.4, "legendary": 0.25, "mythic": 0.05},
                    cooldown_hours=24,
                    max_per_day=1,
                    cost_multiplier=8.0
                )
            }
            
            # Cache no Redis
            configs_data = {k.value: asdict(v) for k, v in default_configs.items()}
            self.redis_client.setex(
                "chest_configs", 
                self._cache_ttl, 
                json.dumps(configs_data, default=str)
            )
            
            return default_configs
            
        except Exception as e:
            logger.error(f"Erro ao carregar configurações: {e}")
            return {}
    
    def _calculate_potential_score(self, profile: UserProfile) -> Tuple[float, Dict[str, float]]:
        """
        Calcula score de potencial otimizado usando algoritmo ponderado
        
        Fatores considerados:
        - Valor monetário (30%): depósitos, GGR, valor médio de apostas
        - Atividade (25%): frequência, sessões, última atividade
        - Engajamento (20%): variedade de jogos, uso de bônus
        - Estabilidade (15%): frequência de depósitos vs saques
        - Risco (10%): score de risco (invertido)
        """
        
        factors = {}
        
        # Fator Monetário (30%)
        monetary_score = 0.0
        
        # Depósitos (peso 40% do fator monetário)
        if profile.total_deposits > 0:
            deposit_score = min(profile.total_deposits / 1000.0, 1.0) * 40
            monetary_score += deposit_score
            factors['deposits'] = deposit_score
        
        # GGR (peso 35% do fator monetário)
        if profile.total_ggr > 0:
            ggr_score = min(profile.total_ggr / 500.0, 1.0) * 35
            monetary_score += ggr_score
            factors['ggr'] = ggr_score
        
        # Valor médio de apostas (peso 25% do fator monetário)
        if profile.avg_bet_value > 0:
            bet_score = min(profile.avg_bet_value / 50.0, 1.0) * 25
            monetary_score += bet_score
            factors['avg_bet'] = bet_score
        
        monetary_factor = monetary_score * 0.30
        
        # Fator Atividade (25%)
        activity_score = 0.0
        
        # Frequência de atividade (peso 50% do fator atividade)
        activity_freq_score = min(profile.activity_frequency / 30.0, 1.0) * 50
        activity_score += activity_freq_score
        factors['activity_frequency'] = activity_freq_score
        
        # Última atividade (peso 30% do fator atividade)
        days_since_last = (datetime.utcnow() - profile.last_activity).days
        recency_score = max(0, (30 - days_since_last) / 30.0) * 30
        activity_score += recency_score
        factors['recency'] = recency_score
        
        # Duração média de sessão (peso 20% do fator atividade)
        session_score = min(profile.session_duration_avg / 60.0, 1.0) * 20
        activity_score += session_score
        factors['session_duration'] = session_score
        
        activity_factor = activity_score * 0.25
        
        # Fator Engajamento (20%)
        engagement_score = 0.0
        
        # Variedade de jogos (peso 60% do fator engajamento)
        game_variety_score = min(len(profile.preferred_games) / 10.0, 1.0) * 60
        engagement_score += game_variety_score
        factors['game_variety'] = game_variety_score
        
        # Uso de bônus (peso 40% do fator engajamento)
        bonus_score = profile.bonus_usage_rate * 40
        engagement_score += bonus_score
        factors['bonus_usage'] = bonus_score
        
        engagement_factor = engagement_score * 0.20
        
        # Fator Estabilidade (15%)
        stability_score = 0.0
        
        # Relação depósitos/saques (peso 100% do fator estabilidade)
        if profile.withdrawal_frequency > 0:
            deposit_withdrawal_ratio = profile.deposit_frequency / profile.withdrawal_frequency
            stability_score = min(deposit_withdrawal_ratio / 3.0, 1.0) * 100
        else:
            stability_score = 100  # Sem saques é estável
        
        factors['stability'] = stability_score
        stability_factor = stability_score * 0.15
        
        # Fator Risco (10% - invertido)
        risk_factor = (1.0 - profile.risk_score) * 10 * 0.10
        factors['risk'] = risk_factor
        
        # Score final
        total_score = monetary_factor + activity_factor + engagement_factor + stability_factor + risk_factor
        
        # Normaliza para 0-100
        final_score = min(max(total_score, 0.0), 100.0)
        
        # Adiciona fatores normalizados
        factors.update({
            'monetary_factor': monetary_factor,
            'activity_factor': activity_factor,
            'engagement_factor': engagement_factor,
            'stability_factor': stability_factor,
            'risk_factor': risk_factor,
            'final_score': final_score
        })
        
        return final_score, factors
    
    def _determine_potential_level(self, score: float) -> PotentialLevel:
        """Determina nível de potencial baseado no score"""
        if score >= 80:
            return PotentialLevel.VERY_HIGH
        elif score >= 60:
            return PotentialLevel.HIGH
        elif score >= 40:
            return PotentialLevel.MEDIUM
        elif score >= 20:
            return PotentialLevel.LOW
        else:
            return PotentialLevel.VERY_LOW
    
    def _recommend_chest(self, score: float, configs: Dict[ChestType, ChestConfig]) -> ChestType:
        """Recomenda tipo de baú baseado no score"""
        for chest_type, config in configs.items():
            if config.min_potential_score <= score <= config.max_potential_score:
                return chest_type
        
        # Fallback para bronze se não encontrar
        return ChestType.BRONZE
    
    def _calculate_confidence(self, profile: UserProfile, score: float) -> float:
        """Calcula confiança da análise baseada na qualidade dos dados"""
        confidence_factors = []
        
        # Quantidade de dados históricos
        days_since_registration = (datetime.utcnow() - profile.registration_date).days
        history_confidence = min(days_since_registration / 90.0, 1.0)
        confidence_factors.append(history_confidence)
        
        # Atividade recente
        days_since_last = (datetime.utcnow() - profile.last_activity).days
        recency_confidence = max(0, (7 - days_since_last) / 7.0)
        confidence_factors.append(recency_confidence)
        
        # Volume de transações
        transaction_confidence = min((profile.total_bets + profile.deposit_frequency) / 50.0, 1.0)
        confidence_factors.append(transaction_confidence)
        
        # Consistência do score (se muito extremo, menor confiança)
        score_consistency = 1.0 - abs(score - 50.0) / 50.0
        confidence_factors.append(score_consistency)
        
        # Média ponderada
        return sum(confidence_factors) / len(confidence_factors)
    
    def analyze_user_potential(self, profile: UserProfile, use_cache: bool = True) -> PotentialAnalysis:
        """Analisa potencial de um usuário com otimizações"""
        start_time = time.time()
        
        try:
            # Verifica cache primeiro
            cache_key = f"potential_analysis:{profile.user_id}"
            
            if use_cache:
                cached_result = self.redis_client.get(cache_key)
                if cached_result:
                    self.metrics['cache_hits'] += 1
                    logger.debug(f"Cache hit para usuário {profile.user_id}")
                    
                    data = json.loads(cached_result)
                    return PotentialAnalysis(
                        user_id=data['user_id'],
                        potential_level=PotentialLevel(data['potential_level']),
                        potential_score=data['potential_score'],
                        recommended_chest=ChestType(data['recommended_chest']),
                        confidence=data['confidence'],
                        factors=data['factors'],
                        analysis_timestamp=datetime.fromisoformat(data['analysis_timestamp']),
                        cache_ttl=data['cache_ttl']
                    )
            
            self.metrics['cache_misses'] += 1
            
            # Carrega configurações
            chest_configs = self._load_chest_configs()
            
            # Calcula score de potencial
            score, factors = self._calculate_potential_score(profile)
            
            # Determina nível e recomendação
            potential_level = self._determine_potential_level(score)
            recommended_chest = self._recommend_chest(score, chest_configs)
            confidence = self._calculate_confidence(profile, score)
            
            # Cria resultado
            analysis = PotentialAnalysis(
                user_id=profile.user_id,
                potential_level=potential_level,
                potential_score=score,
                recommended_chest=recommended_chest,
                confidence=confidence,
                factors=factors,
                analysis_timestamp=datetime.utcnow(),
                cache_ttl=self._cache_ttl
            )
            
            # Cache o resultado
            if use_cache:
                cache_data = {
                    'user_id': analysis.user_id,
                    'potential_level': analysis.potential_level.value,
                    'potential_score': analysis.potential_score,
                    'recommended_chest': analysis.recommended_chest.value,
                    'confidence': analysis.confidence,
                    'factors': analysis.factors,
                    'analysis_timestamp': analysis.analysis_timestamp.isoformat(),
                    'cache_ttl': analysis.cache_ttl
                }
                
                self.redis_client.setex(
                    cache_key,
                    self._cache_ttl,
                    json.dumps(cache_data, default=str)
                )
            
            # Atualiza métricas
            analysis_time = time.time() - start_time
            self.metrics['analyses_performed'] += 1
            self.metrics['total_analysis_time'] += analysis_time
            self.metrics['avg_analysis_time'] = (
                self.metrics['total_analysis_time'] / self.metrics['analyses_performed']
            )
            
            logger.info(f"Análise concluída para {profile.user_id}: {score:.2f} ({potential_level.value}) em {analysis_time:.3f}s")
            
            return analysis
            
        except Exception as e:
            logger.error(f"Erro na análise de potencial para {profile.user_id}: {e}")
            raise
    
    async def analyze_batch_async(self, profiles: List[UserProfile], use_cache: bool = True) -> List[PotentialAnalysis]:
        """Analisa múltiplos usuários de forma assíncrona"""
        
        async def analyze_single(profile):
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                self.thread_pool,
                self.analyze_user_potential,
                profile,
                use_cache
            )
        
        logger.info(f"Iniciando análise em lote de {len(profiles)} usuários")
        start_time = time.time()
        
        # Executa análises em paralelo
        tasks = [analyze_single(profile) for profile in profiles]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filtra erros
        successful_results = []
        errors = []
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                errors.append(f"Erro no usuário {profiles[i].user_id}: {result}")
            else:
                successful_results.append(result)
        
        total_time = time.time() - start_time
        
        logger.info(f"Análise em lote concluída: {len(successful_results)}/{len(profiles)} sucessos em {total_time:.2f}s")
        
        if errors:
            logger.warning(f"Erros encontrados: {errors}")
        
        return successful_results
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Retorna métricas de performance do analisador"""
        cache_hit_rate = 0.0
        if self.metrics['cache_hits'] + self.metrics['cache_misses'] > 0:
            cache_hit_rate = self.metrics['cache_hits'] / (
                self.metrics['cache_hits'] + self.metrics['cache_misses']
            )
        
        return {
            'analyses_performed': self.metrics['analyses_performed'],
            'cache_hit_rate': cache_hit_rate,
            'avg_analysis_time_ms': self.metrics['avg_analysis_time'] * 1000,
            'total_analysis_time': self.metrics['total_analysis_time'],
            'cache_hits': self.metrics['cache_hits'],
            'cache_misses': self.metrics['cache_misses']
        }
    
    def clear_cache(self, user_id: str = None):
        """Limpa cache de análises"""
        if user_id:
            cache_key = f"potential_analysis:{user_id}"
            self.redis_client.delete(cache_key)
            logger.info(f"Cache limpo para usuário {user_id}")
        else:
            # Limpa todos os caches de análise
            keys = self.redis_client.keys("potential_analysis:*")
            if keys:
                self.redis_client.delete(*keys)
                logger.info(f"Cache limpo para {len(keys)} usuários")

# Exemplo de uso e testes
if __name__ == "__main__":
    # Mock do Redis para teste
    class MockRedis:
        def __init__(self):
            self.data = {}
        
        def get(self, key):
            return self.data.get(key)
        
        def setex(self, key, ttl, value):
            self.data[key] = value
        
        def delete(self, *keys):
            for key in keys:
                self.data.pop(key, None)
        
        def keys(self, pattern):
            return [k for k in self.data.keys() if pattern.replace('*', '') in k]
    
    # Inicializa analisador
    mock_redis = MockRedis()
    analyzer = OptimizedChestAnalyzer(redis_client=mock_redis)
    
    # Perfil de teste - usuário de alto potencial
    high_potential_user = UserProfile(
        user_id="user_001",
        registration_date=datetime.utcnow() - timedelta(days=60),
        total_deposits=2500.00,
        total_bets=150,
        total_ggr=800.00,
        avg_bet_value=25.00,
        last_activity=datetime.utcnow() - timedelta(hours=2),
        activity_frequency=25,
        preferred_games=["slots", "blackjack", "roulette", "poker"],
        deposit_frequency=8,
        withdrawal_frequency=2,
        bonus_usage_rate=0.8,
        session_duration_avg=45.0,
        device_types=["mobile", "desktop"],
        geographic_region="BR-SP",
        risk_score=0.2
    )
    
    # Perfil de teste - usuário de baixo potencial
    low_potential_user = UserProfile(
        user_id="user_002",
        registration_date=datetime.utcnow() - timedelta(days=5),
        total_deposits=50.00,
        total_bets=5,
        total_ggr=10.00,
        avg_bet_value=2.00,
        last_activity=datetime.utcnow() - timedelta(days=3),
        activity_frequency=3,
        preferred_games=["slots"],
        deposit_frequency=1,
        withdrawal_frequency=0,
        bonus_usage_rate=0.3,
        session_duration_avg=15.0,
        device_types=["mobile"],
        geographic_region="BR-RJ",
        risk_score=0.7
    )
    
    print("=== Teste do Sistema Otimizado de Análise de Baús ===")
    
    # Teste de análise individual
    print("\n1. Análise Individual:")
    
    analysis_1 = analyzer.analyze_user_potential(high_potential_user)
    print(f"Usuário {analysis_1.user_id}:")
    print(f"  Score: {analysis_1.potential_score:.2f}")
    print(f"  Nível: {analysis_1.potential_level.value}")
    print(f"  Baú Recomendado: {analysis_1.recommended_chest.value}")
    print(f"  Confiança: {analysis_1.confidence:.2f}")
    
    analysis_2 = analyzer.analyze_user_potential(low_potential_user)
    print(f"Usuário {analysis_2.user_id}:")
    print(f"  Score: {analysis_2.potential_score:.2f}")
    print(f"  Nível: {analysis_2.potential_level.value}")
    print(f"  Baú Recomendado: {analysis_2.recommended_chest.value}")
    print(f"  Confiança: {analysis_2.confidence:.2f}")
    
    # Teste de cache
    print("\n2. Teste de Cache:")
    start_time = time.time()
    analyzer.analyze_user_potential(high_potential_user)  # Deve usar cache
    cache_time = time.time() - start_time
    print(f"Tempo com cache: {cache_time:.4f}s")
    
    # Teste de análise em lote
    print("\n3. Análise em Lote:")
    
    async def test_batch():
        profiles = [high_potential_user, low_potential_user] * 5  # 10 usuários
        results = await analyzer.analyze_batch_async(profiles)
        return results
    
    batch_results = asyncio.run(test_batch())
    print(f"Análises em lote: {len(batch_results)} resultados")
    
    # Métricas de performance
    print("\n4. Métricas de Performance:")
    metrics = analyzer.get_performance_metrics()
    for key, value in metrics.items():
        print(f"  {key}: {value}")
    
    print("\n✅ Testes concluídos com sucesso!")

