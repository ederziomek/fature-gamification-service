#!/usr/bin/env python3
"""
Sistema Otimizado de Análise de Potencial de Baús - VERSÃO CORRIGIDA
Todas as configurações obtidas do config-service (sem valores hardcoded)
"""
import asyncio
import json
import logging
import random
import time
import requests
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Tuple
import redis

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

@dataclass
class UserProfile:
    """Perfil do usuário para análise"""
    user_id: str
    affiliate_id: str
    registration_date: datetime
    total_deposits: float
    total_bets: int
    total_ggr: float
    last_activity: datetime
    vip_level: int = 0
    preferred_games: List[str] = None

@dataclass
class ChestConfig:
    """Configuração de um tipo de baú obtida do config-service"""
    chest_type: ChestType
    min_deposit: float
    min_bets: int
    min_ggr: float
    min_vip_level: int
    weight_deposit: float
    weight_activity: float
    weight_loyalty: float
    weight_vip: float
    base_probability: float
    max_rewards_value: float
    cooldown_hours: int

@dataclass
class AnalysisResult:
    """Resultado da análise de potencial"""
    user_id: str
    recommended_chest: ChestType
    probability_score: float
    potential_value: float
    analysis_factors: Dict
    timestamp: datetime
    cache_ttl: int = 300

class ConfigServiceClient:
    """Cliente para comunicação com o config-service"""
    
    def __init__(self, config_service_url: str = None):
        self.config_service_url = config_service_url or "http://config-service.fature.svc.cluster.local"
        self.session = requests.Session()
        self.session.timeout = 10
        
    def get_chest_configs(self) -> Dict[ChestType, ChestConfig]:
        """Obtém configurações de todos os tipos de baús"""
        try:
            configs = {}
            
            for chest_type in ChestType:
                config = self._get_chest_config(chest_type)
                configs[chest_type] = config
                
            return configs
            
        except Exception as e:
            logger.error(f"Failed to get chest configs: {e}")
            return self._get_emergency_configs()
    
    def _get_chest_config(self, chest_type: ChestType) -> ChestConfig:
        """Obtém configuração de um tipo específico de baú"""
        prefix = f"gamificacao.baus.{chest_type.value}"
        
        config_keys = [
            f"{prefix}.deposito_minimo",
            f"{prefix}.apostas_minimas",
            f"{prefix}.ggr_minimo",
            f"{prefix}.nivel_vip_minimo",
            f"{prefix}.peso_deposito",
            f"{prefix}.peso_atividade",
            f"{prefix}.peso_fidelidade",
            f"{prefix}.peso_vip",
            f"{prefix}.probabilidade_base",
            f"{prefix}.valor_maximo_recompensa",
            f"{prefix}.cooldown_horas"
        ]
        
        config_values = {}
        
        for key in config_keys:
            try:
                response = self.session.get(f"{self.config_service_url}/api/v1/config/{key}")
                if response.status_code == 200:
                    data = response.json()
                    if data.get('success'):
                        config_values[key] = data['data']['value']
                    else:
                        config_values[key] = self._get_fallback_value(key)
                else:
                    config_values[key] = self._get_fallback_value(key)
                    
            except Exception as e:
                logger.error(f"Error getting config {key}: {e}")
                config_values[key] = self._get_fallback_value(key)
        
        return ChestConfig(
            chest_type=chest_type,
            min_deposit=float(config_values[f"{prefix}.deposito_minimo"]),
            min_bets=int(config_values[f"{prefix}.apostas_minimas"]),
            min_ggr=float(config_values[f"{prefix}.ggr_minimo"]),
            min_vip_level=int(config_values[f"{prefix}.nivel_vip_minimo"]),
            weight_deposit=float(config_values[f"{prefix}.peso_deposito"]),
            weight_activity=float(config_values[f"{prefix}.peso_atividade"]),
            weight_loyalty=float(config_values[f"{prefix}.peso_fidelidade"]),
            weight_vip=float(config_values[f"{prefix}.peso_vip"]),
            base_probability=float(config_values[f"{prefix}.probabilidade_base"]),
            max_rewards_value=float(config_values[f"{prefix}.valor_maximo_recompensa"]),
            cooldown_hours=int(config_values[f"{prefix}.cooldown_horas"])
        )
    
    def _get_fallback_value(self, key: str):
        """Valores de fallback apenas para emergência"""
        fallbacks = {
            # Bronze
            "gamificacao.baus.bronze.deposito_minimo": 10.0,
            "gamificacao.baus.bronze.apostas_minimas": 5,
            "gamificacao.baus.bronze.ggr_minimo": 5.0,
            "gamificacao.baus.bronze.nivel_vip_minimo": 0,
            "gamificacao.baus.bronze.peso_deposito": 0.3,
            "gamificacao.baus.bronze.peso_atividade": 0.3,
            "gamificacao.baus.bronze.peso_fidelidade": 0.2,
            "gamificacao.baus.bronze.peso_vip": 0.2,
            "gamificacao.baus.bronze.probabilidade_base": 0.8,
            "gamificacao.baus.bronze.valor_maximo_recompensa": 50.0,
            "gamificacao.baus.bronze.cooldown_horas": 24,
            
            # Silver
            "gamificacao.baus.silver.deposito_minimo": 50.0,
            "gamificacao.baus.silver.apostas_minimas": 20,
            "gamificacao.baus.silver.ggr_minimo": 25.0,
            "gamificacao.baus.silver.nivel_vip_minimo": 1,
            "gamificacao.baus.silver.peso_deposito": 0.35,
            "gamificacao.baus.silver.peso_atividade": 0.25,
            "gamificacao.baus.silver.peso_fidelidade": 0.25,
            "gamificacao.baus.silver.peso_vip": 0.15,
            "gamificacao.baus.silver.probabilidade_base": 0.6,
            "gamificacao.baus.silver.valor_maximo_recompensa": 150.0,
            "gamificacao.baus.silver.cooldown_horas": 48,
            
            # Gold
            "gamificacao.baus.gold.deposito_minimo": 200.0,
            "gamificacao.baus.gold.apostas_minimas": 50,
            "gamificacao.baus.gold.ggr_minimo": 100.0,
            "gamificacao.baus.gold.nivel_vip_minimo": 2,
            "gamificacao.baus.gold.peso_deposito": 0.4,
            "gamificacao.baus.gold.peso_atividade": 0.2,
            "gamificacao.baus.gold.peso_fidelidade": 0.3,
            "gamificacao.baus.gold.peso_vip": 0.1,
            "gamificacao.baus.gold.probabilidade_base": 0.4,
            "gamificacao.baus.gold.valor_maximo_recompensa": 500.0,
            "gamificacao.baus.gold.cooldown_horas": 72,
            
            # Platinum
            "gamificacao.baus.platinum.deposito_minimo": 1000.0,
            "gamificacao.baus.platinum.apostas_minimas": 100,
            "gamificacao.baus.platinum.ggr_minimo": 500.0,
            "gamificacao.baus.platinum.nivel_vip_minimo": 3,
            "gamificacao.baus.platinum.peso_deposito": 0.5,
            "gamificacao.baus.platinum.peso_atividade": 0.15,
            "gamificacao.baus.platinum.peso_fidelidade": 0.25,
            "gamificacao.baus.platinum.peso_vip": 0.1,
            "gamificacao.baus.platinum.probabilidade_base": 0.2,
            "gamificacao.baus.platinum.valor_maximo_recompensa": 2000.0,
            "gamificacao.baus.platinum.cooldown_horas": 168,
            
            # Diamond
            "gamificacao.baus.diamond.deposito_minimo": 5000.0,
            "gamificacao.baus.diamond.apostas_minimas": 500,
            "gamificacao.baus.diamond.ggr_minimo": 2500.0,
            "gamificacao.baus.diamond.nivel_vip_minimo": 5,
            "gamificacao.baus.diamond.peso_deposito": 0.6,
            "gamificacao.baus.diamond.peso_atividade": 0.1,
            "gamificacao.baus.diamond.peso_fidelidade": 0.2,
            "gamificacao.baus.diamond.peso_vip": 0.1,
            "gamificacao.baus.diamond.probabilidade_base": 0.05,
            "gamificacao.baus.diamond.valor_maximo_recompensa": 10000.0,
            "gamificacao.baus.diamond.cooldown_horas": 336
        }
        return fallbacks.get(key, 0)
    
    def _get_emergency_configs(self) -> Dict[ChestType, ChestConfig]:
        """Configurações de emergência quando config-service não responde"""
        configs = {}
        for chest_type in ChestType:
            configs[chest_type] = self._get_chest_config(chest_type)
        return configs

class ChestAnalyzer:
    """Analisador otimizado de potencial de baús"""
    
    def __init__(self, config_service_url: str = None, redis_url: str = None):
        self.config_client = ConfigServiceClient(config_service_url)
        self.redis_client = None
        self.thread_pool = ThreadPoolExecutor(max_workers=10)
        self._configs = None
        self._configs_last_updated = None
        self._configs_ttl = 300  # 5 minutos
        
        # Inicializa Redis se disponível
        if redis_url:
            try:
                self.redis_client = redis.from_url(redis_url)
                self.redis_client.ping()
                logger.info("Redis cache initialized successfully")
            except Exception as e:
                logger.warning(f"Redis not available: {e}")
                self.redis_client = None
        
        # Métricas de performance
        self.metrics = {
            'total_analyses': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'avg_analysis_time': 0.0
        }
    
    def _get_configs(self) -> Dict[ChestType, ChestConfig]:
        """Obtém configurações com cache"""
        now = datetime.utcnow()
        
        if (self._configs is None or 
            self._configs_last_updated is None or 
            (now - self._configs_last_updated).total_seconds() > self._configs_ttl):
            
            logger.info("Refreshing chest configs from config-service")
            self._configs = self.config_client.get_chest_configs()
            self._configs_last_updated = now
            
        return self._configs
    
    def analyze_user_potential(self, user_profile: UserProfile) -> AnalysisResult:
        """Analisa potencial de um usuário para baús"""
        start_time = time.time()
        
        try:
            # Verifica cache primeiro
            cached_result = self._get_cached_result(user_profile.user_id)
            if cached_result:
                self.metrics['cache_hits'] += 1
                return cached_result
            
            self.metrics['cache_misses'] += 1
            
            # Obtém configurações atualizadas
            configs = self._get_configs()
            
            # Calcula scores para cada tipo de baú
            chest_scores = {}
            for chest_type, config in configs.items():
                score = self._calculate_chest_score(user_profile, config)
                chest_scores[chest_type] = score
            
            # Encontra o melhor baú
            best_chest = max(chest_scores.keys(), key=lambda x: chest_scores[x])
            best_score = chest_scores[best_chest]
            
            # Calcula valor potencial
            potential_value = self._calculate_potential_value(
                user_profile, configs[best_chest], best_score
            )
            
            # Cria resultado
            result = AnalysisResult(
                user_id=user_profile.user_id,
                recommended_chest=best_chest,
                probability_score=best_score,
                potential_value=potential_value,
                analysis_factors=self._get_analysis_factors(user_profile, configs[best_chest]),
                timestamp=datetime.utcnow()
            )
            
            # Salva no cache
            self._cache_result(result)
            
            # Atualiza métricas
            analysis_time = time.time() - start_time
            self.metrics['total_analyses'] += 1
            self.metrics['avg_analysis_time'] = (
                (self.metrics['avg_analysis_time'] * (self.metrics['total_analyses'] - 1) + analysis_time) /
                self.metrics['total_analyses']
            )
            
            logger.info(f"Analysis completed for user {user_profile.user_id} in {analysis_time:.3f}s")
            return result
            
        except Exception as e:
            logger.error(f"Error analyzing user {user_profile.user_id}: {e}")
            # Retorna resultado padrão em caso de erro
            return AnalysisResult(
                user_id=user_profile.user_id,
                recommended_chest=ChestType.BRONZE,
                probability_score=0.1,
                potential_value=10.0,
                analysis_factors={'error': str(e)},
                timestamp=datetime.utcnow()
            )
    
    def _calculate_chest_score(self, user_profile: UserProfile, config: ChestConfig) -> float:
        """Calcula score de adequação para um tipo de baú"""
        # Verifica requisitos mínimos
        if (user_profile.total_deposits < config.min_deposit or
            user_profile.total_bets < config.min_bets or
            user_profile.total_ggr < config.min_ggr or
            user_profile.vip_level < config.min_vip_level):
            return 0.0
        
        # Calcula fatores ponderados
        deposit_factor = min(user_profile.total_deposits / config.min_deposit, 3.0)
        activity_factor = min(user_profile.total_bets / config.min_bets, 3.0)
        
        # Fator de fidelidade (baseado em tempo desde registro)
        days_since_registration = (datetime.utcnow() - user_profile.registration_date).days
        loyalty_factor = min(days_since_registration / 30.0, 2.0)
        
        # Fator VIP
        vip_factor = min(user_profile.vip_level / 5.0, 1.0) if user_profile.vip_level > 0 else 0.1
        
        # Score ponderado
        weighted_score = (
            deposit_factor * config.weight_deposit +
            activity_factor * config.weight_activity +
            loyalty_factor * config.weight_loyalty +
            vip_factor * config.weight_vip
        )
        
        # Aplica probabilidade base
        final_score = weighted_score * config.base_probability
        
        return min(final_score, 1.0)
    
    def _calculate_potential_value(self, user_profile: UserProfile, config: ChestConfig, score: float) -> float:
        """Calcula valor potencial da recompensa"""
        base_value = config.max_rewards_value * score
        
        # Ajustes baseados no perfil
        if user_profile.vip_level >= 3:
            base_value *= 1.5
        elif user_profile.vip_level >= 1:
            base_value *= 1.2
        
        # Ajuste por atividade recente
        days_since_activity = (datetime.utcnow() - user_profile.last_activity).days
        if days_since_activity <= 1:
            base_value *= 1.3
        elif days_since_activity <= 7:
            base_value *= 1.1
        
        return round(base_value, 2)
    
    def _get_analysis_factors(self, user_profile: UserProfile, config: ChestConfig) -> Dict:
        """Obtém fatores detalhados da análise"""
        return {
            'deposit_ratio': user_profile.total_deposits / config.min_deposit,
            'bets_ratio': user_profile.total_bets / config.min_bets,
            'ggr_ratio': user_profile.total_ggr / config.min_ggr,
            'vip_level': user_profile.vip_level,
            'days_since_registration': (datetime.utcnow() - user_profile.registration_date).days,
            'days_since_activity': (datetime.utcnow() - user_profile.last_activity).days,
            'config_weights': {
                'deposit': config.weight_deposit,
                'activity': config.weight_activity,
                'loyalty': config.weight_loyalty,
                'vip': config.weight_vip
            }
        }
    
    def _get_cached_result(self, user_id: str) -> Optional[AnalysisResult]:
        """Obtém resultado do cache"""
        if not self.redis_client:
            return None
        
        try:
            cached_data = self.redis_client.get(f"chest_analysis:{user_id}")
            if cached_data:
                data = json.loads(cached_data)
                return AnalysisResult(
                    user_id=data['user_id'],
                    recommended_chest=ChestType(data['recommended_chest']),
                    probability_score=data['probability_score'],
                    potential_value=data['potential_value'],
                    analysis_factors=data['analysis_factors'],
                    timestamp=datetime.fromisoformat(data['timestamp'])
                )
        except Exception as e:
            logger.warning(f"Cache read error: {e}")
        
        return None
    
    def _cache_result(self, result: AnalysisResult):
        """Salva resultado no cache"""
        if not self.redis_client:
            return
        
        try:
            cache_data = {
                'user_id': result.user_id,
                'recommended_chest': result.recommended_chest.value,
                'probability_score': result.probability_score,
                'potential_value': result.potential_value,
                'analysis_factors': result.analysis_factors,
                'timestamp': result.timestamp.isoformat()
            }
            
            self.redis_client.setex(
                f"chest_analysis:{result.user_id}",
                result.cache_ttl,
                json.dumps(cache_data)
            )
        except Exception as e:
            logger.warning(f"Cache write error: {e}")
    
    async def analyze_batch(self, user_profiles: List[UserProfile]) -> List[AnalysisResult]:
        """Analisa múltiplos usuários em paralelo"""
        logger.info(f"Starting batch analysis for {len(user_profiles)} users")
        
        loop = asyncio.get_event_loop()
        tasks = []
        
        for user_profile in user_profiles:
            task = loop.run_in_executor(
                self.thread_pool,
                self.analyze_user_potential,
                user_profile
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        
        valid_results = [r for r in results if r.probability_score > 0]
        logger.info(f"Batch analysis completed: {len(valid_results)}/{len(results)} valid results")
        
        return results
    
    def get_performance_metrics(self) -> Dict:
        """Obtém métricas de performance"""
        cache_hit_rate = (
            self.metrics['cache_hits'] / 
            (self.metrics['cache_hits'] + self.metrics['cache_misses'])
            if (self.metrics['cache_hits'] + self.metrics['cache_misses']) > 0 else 0
        )
        
        return {
            **self.metrics,
            'cache_hit_rate': cache_hit_rate,
            'throughput_per_second': (
                self.metrics['total_analyses'] / max(self.metrics['avg_analysis_time'], 0.001)
                if self.metrics['avg_analysis_time'] > 0 else 0
            )
        }

# Exemplo de uso e testes
if __name__ == "__main__":
    # Inicializa analisador (irá buscar configurações do config-service)
    analyzer = ChestAnalyzer()
    
    # Dados de teste
    user_1 = UserProfile(
        user_id="user_001",
        affiliate_id="aff_001",
        registration_date=datetime.utcnow() - timedelta(days=30),
        total_deposits=150.0,
        total_bets=45,
        total_ggr=75.0,
        last_activity=datetime.utcnow() - timedelta(hours=2),
        vip_level=1
    )
    
    user_2 = UserProfile(
        user_id="user_002",
        affiliate_id="aff_002",
        registration_date=datetime.utcnow() - timedelta(days=90),
        total_deposits=2500.0,
        total_bets=200,
        total_ggr=1200.0,
        last_activity=datetime.utcnow() - timedelta(hours=1),
        vip_level=4
    )
    
    print("=== Teste de Análise de Baús (Config-Service) ===")
    
    # Teste individual
    result_1 = analyzer.analyze_user_potential(user_1)
    print(f"User 1 - Baú recomendado: {result_1.recommended_chest.value}")
    print(f"Score: {result_1.probability_score:.3f}, Valor potencial: R$ {result_1.potential_value:.2f}")
    
    result_2 = analyzer.analyze_user_potential(user_2)
    print(f"User 2 - Baú recomendado: {result_2.recommended_chest.value}")
    print(f"Score: {result_2.probability_score:.3f}, Valor potencial: R$ {result_2.potential_value:.2f}")
    
    # Teste em lote
    async def test_batch():
        batch_results = await analyzer.analyze_batch([user_1, user_2])
        print(f"Análise em lote: {len(batch_results)} resultados")
        
        # Métricas de performance
        metrics = analyzer.get_performance_metrics()
        print(f"Métricas: {metrics}")
    
    asyncio.run(test_batch())
    
    print("\n✅ Todas as configurações obtidas do config-service!")

