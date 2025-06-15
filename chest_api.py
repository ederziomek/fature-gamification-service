#!/usr/bin/env python3
"""
API REST para Sistema Otimizado de Análise de Baús
Microserviço de alta performance para análise de potencial
"""
from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime
import logging
import json
import asyncio
from typing import Dict, List
import redis

# Importa o analisador otimizado
from chest_analyzer import (
    OptimizedChestAnalyzer,
    UserProfile,
    PotentialAnalysis,
    ChestType,
    PotentialLevel
)

# Configuração da aplicação
app = Flask(__name__)
CORS(app)

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Instância global do analisador
try:
    redis_client = redis.Redis(
        host='redis.fature.svc.cluster.local',
        port=6379,
        decode_responses=True,
        socket_timeout=5,
        socket_connect_timeout=5
    )
    # Testa conexão
    redis_client.ping()
    logger.info("Conectado ao Redis com sucesso")
except Exception as e:
    logger.warning(f"Erro ao conectar Redis: {e}. Usando cache em memória.")
    redis_client = None

analyzer = OptimizedChestAnalyzer(redis_client=redis_client)

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    redis_status = "connected" if redis_client else "disconnected"
    
    return jsonify({
        'status': 'healthy',
        'service': 'chest-analyzer-service',
        'timestamp': datetime.utcnow().isoformat(),
        'version': '1.0.0',
        'redis_status': redis_status,
        'performance_metrics': analyzer.get_performance_metrics()
    })

@app.route('/api/v1/analyze', methods=['POST'])
def analyze_user_potential():
    """Analisa potencial de um usuário"""
    try:
        data = request.get_json()
        
        # Validação dos dados de entrada
        required_fields = [
            'user_id', 'registration_date', 'total_deposits', 'total_bets',
            'total_ggr', 'avg_bet_value', 'last_activity', 'activity_frequency'
        ]
        
        for field in required_fields:
            if field not in data:
                return jsonify({
                    'success': False,
                    'error': f'Missing required field: {field}'
                }), 400
        
        # Converte datas
        try:
            registration_date = datetime.fromisoformat(data['registration_date'].replace('Z', '+00:00'))
            last_activity = datetime.fromisoformat(data['last_activity'].replace('Z', '+00:00'))
        except ValueError as e:
            return jsonify({
                'success': False,
                'error': f'Invalid date format: {e}'
            }), 400
        
        # Cria perfil do usuário
        profile = UserProfile(
            user_id=data['user_id'],
            registration_date=registration_date,
            total_deposits=float(data['total_deposits']),
            total_bets=int(data['total_bets']),
            total_ggr=float(data['total_ggr']),
            avg_bet_value=float(data['avg_bet_value']),
            last_activity=last_activity,
            activity_frequency=int(data['activity_frequency']),
            preferred_games=data.get('preferred_games', []),
            deposit_frequency=int(data.get('deposit_frequency', 0)),
            withdrawal_frequency=int(data.get('withdrawal_frequency', 0)),
            bonus_usage_rate=float(data.get('bonus_usage_rate', 0.0)),
            session_duration_avg=float(data.get('session_duration_avg', 0.0)),
            device_types=data.get('device_types', []),
            geographic_region=data.get('geographic_region', ''),
            risk_score=float(data.get('risk_score', 0.5))
        )
        
        # Analisa potencial
        use_cache = data.get('use_cache', True)
        analysis = analyzer.analyze_user_potential(profile, use_cache=use_cache)
        
        # Resposta
        response_data = {
            'success': True,
            'data': {
                'user_id': analysis.user_id,
                'potential_level': analysis.potential_level.value,
                'potential_score': analysis.potential_score,
                'recommended_chest': analysis.recommended_chest.value,
                'confidence': analysis.confidence,
                'factors': analysis.factors,
                'analysis_timestamp': analysis.analysis_timestamp.isoformat(),
                'cache_ttl': analysis.cache_ttl
            }
        }
        
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"Erro na análise: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Internal server error: {str(e)}'
        }), 500

@app.route('/api/v1/analyze/batch', methods=['POST'])
def analyze_batch_potential():
    """Analisa potencial de múltiplos usuários em lote"""
    try:
        data = request.get_json()
        
        if 'users' not in data:
            return jsonify({
                'success': False,
                'error': 'Missing users array'
            }), 400
        
        if len(data['users']) > 100:
            return jsonify({
                'success': False,
                'error': 'Batch size limit exceeded (max 100 users)'
            }), 400
        
        profiles = []
        errors = []
        
        # Processa cada usuário
        for i, user_data in enumerate(data['users']):
            try:
                # Validação básica
                required_fields = [
                    'user_id', 'registration_date', 'total_deposits', 'total_bets',
                    'total_ggr', 'avg_bet_value', 'last_activity', 'activity_frequency'
                ]
                
                for field in required_fields:
                    if field not in user_data:
                        errors.append(f'User {i}: Missing field {field}')
                        continue
                
                # Converte datas
                registration_date = datetime.fromisoformat(user_data['registration_date'].replace('Z', '+00:00'))
                last_activity = datetime.fromisoformat(user_data['last_activity'].replace('Z', '+00:00'))
                
                # Cria perfil
                profile = UserProfile(
                    user_id=user_data['user_id'],
                    registration_date=registration_date,
                    total_deposits=float(user_data['total_deposits']),
                    total_bets=int(user_data['total_bets']),
                    total_ggr=float(user_data['total_ggr']),
                    avg_bet_value=float(user_data['avg_bet_value']),
                    last_activity=last_activity,
                    activity_frequency=int(user_data['activity_frequency']),
                    preferred_games=user_data.get('preferred_games', []),
                    deposit_frequency=int(user_data.get('deposit_frequency', 0)),
                    withdrawal_frequency=int(user_data.get('withdrawal_frequency', 0)),
                    bonus_usage_rate=float(user_data.get('bonus_usage_rate', 0.0)),
                    session_duration_avg=float(user_data.get('session_duration_avg', 0.0)),
                    device_types=user_data.get('device_types', []),
                    geographic_region=user_data.get('geographic_region', ''),
                    risk_score=float(user_data.get('risk_score', 0.5))
                )
                
                profiles.append(profile)
                
            except Exception as e:
                errors.append(f'User {i}: {str(e)}')
        
        if errors:
            return jsonify({
                'success': False,
                'errors': errors
            }), 400
        
        # Análise em lote assíncrona
        use_cache = data.get('use_cache', True)
        
        async def run_batch_analysis():
            return await analyzer.analyze_batch_async(profiles, use_cache=use_cache)
        
        # Executa análise
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            results = loop.run_until_complete(run_batch_analysis())
        finally:
            loop.close()
        
        # Prepara resposta
        response_data = {
            'success': True,
            'data': {
                'total_processed': len(results),
                'high_potential_count': sum(1 for r in results if r.potential_level in [PotentialLevel.HIGH, PotentialLevel.VERY_HIGH]),
                'medium_potential_count': sum(1 for r in results if r.potential_level == PotentialLevel.MEDIUM),
                'low_potential_count': sum(1 for r in results if r.potential_level in [PotentialLevel.LOW, PotentialLevel.VERY_LOW]),
                'results': [
                    {
                        'user_id': r.user_id,
                        'potential_level': r.potential_level.value,
                        'potential_score': r.potential_score,
                        'recommended_chest': r.recommended_chest.value,
                        'confidence': r.confidence,
                        'factors': r.factors,
                        'analysis_timestamp': r.analysis_timestamp.isoformat()
                    }
                    for r in results
                ]
            }
        }
        
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"Erro na análise em lote: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Internal server error: {str(e)}'
        }), 500

@app.route('/api/v1/chest-configs', methods=['GET'])
def get_chest_configs():
    """Retorna configurações dos tipos de baús"""
    try:
        configs = analyzer._load_chest_configs()
        
        configs_data = {}
        for chest_type, config in configs.items():
            configs_data[chest_type.value] = {
                'min_potential_score': config.min_potential_score,
                'max_potential_score': config.max_potential_score,
                'rewards': config.rewards,
                'rarity_weights': config.rarity_weights,
                'cooldown_hours': config.cooldown_hours,
                'max_per_day': config.max_per_day,
                'cost_multiplier': config.cost_multiplier
            }
        
        return jsonify({
            'success': True,
            'data': configs_data
        })
        
    except Exception as e:
        logger.error(f"Erro ao buscar configurações: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Internal server error: {str(e)}'
        }), 500

@app.route('/api/v1/metrics', methods=['GET'])
def get_performance_metrics():
    """Retorna métricas de performance do analisador"""
    try:
        metrics = analyzer.get_performance_metrics()
        
        return jsonify({
            'success': True,
            'data': metrics
        })
        
    except Exception as e:
        logger.error(f"Erro ao buscar métricas: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Internal server error: {str(e)}'
        }), 500

@app.route('/api/v1/cache/clear', methods=['POST'])
def clear_cache():
    """Limpa cache de análises"""
    try:
        data = request.get_json() or {}
        user_id = data.get('user_id')
        
        analyzer.clear_cache(user_id=user_id)
        
        message = f"Cache cleared for user {user_id}" if user_id else "All cache cleared"
        
        return jsonify({
            'success': True,
            'message': message
        })
        
    except Exception as e:
        logger.error(f"Erro ao limpar cache: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Internal server error: {str(e)}'
        }), 500

@app.route('/api/v1/test', methods=['POST'])
def test_analysis():
    """Endpoint de teste para análise"""
    try:
        # Dados de teste padrão
        test_data = {
            'user_id': 'test_user_001',
            'registration_date': (datetime.utcnow() - timedelta(days=30)).isoformat(),
            'total_deposits': 500.00,
            'total_bets': 50,
            'total_ggr': 150.00,
            'avg_bet_value': 10.00,
            'last_activity': datetime.utcnow().isoformat(),
            'activity_frequency': 20,
            'preferred_games': ['slots', 'blackjack'],
            'deposit_frequency': 5,
            'withdrawal_frequency': 1,
            'bonus_usage_rate': 0.6,
            'session_duration_avg': 30.0,
            'device_types': ['mobile'],
            'geographic_region': 'BR-SP',
            'risk_score': 0.3
        }
        
        # Sobrescreve com dados do request se fornecidos
        if request.get_json():
            test_data.update(request.get_json())
        
        # Cria perfil de teste
        profile = UserProfile(
            user_id=test_data['user_id'],
            registration_date=datetime.fromisoformat(test_data['registration_date'].replace('Z', '+00:00')),
            total_deposits=float(test_data['total_deposits']),
            total_bets=int(test_data['total_bets']),
            total_ggr=float(test_data['total_ggr']),
            avg_bet_value=float(test_data['avg_bet_value']),
            last_activity=datetime.fromisoformat(test_data['last_activity'].replace('Z', '+00:00')),
            activity_frequency=int(test_data['activity_frequency']),
            preferred_games=test_data['preferred_games'],
            deposit_frequency=int(test_data['deposit_frequency']),
            withdrawal_frequency=int(test_data['withdrawal_frequency']),
            bonus_usage_rate=float(test_data['bonus_usage_rate']),
            session_duration_avg=float(test_data['session_duration_avg']),
            device_types=test_data['device_types'],
            geographic_region=test_data['geographic_region'],
            risk_score=float(test_data['risk_score'])
        )
        
        # Analisa
        analysis = analyzer.analyze_user_potential(profile, use_cache=False)
        
        return jsonify({
            'success': True,
            'test_data': test_data,
            'analysis_result': {
                'potential_level': analysis.potential_level.value,
                'potential_score': analysis.potential_score,
                'recommended_chest': analysis.recommended_chest.value,
                'confidence': analysis.confidence,
                'factors': analysis.factors
            }
        })
        
    except Exception as e:
        logger.error(f"Erro no teste: {str(e)}")
        return jsonify({
            'success': False,
            'error': f'Internal server error: {str(e)}'
        }), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'success': False,
        'error': 'Endpoint not found'
    }), 404

@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({
        'success': False,
        'error': 'Method not allowed'
    }), 405

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        'success': False,
        'error': 'Internal server error'
    }), 500

if __name__ == '__main__':
    logger.info("Iniciando Chest Analyzer API...")
    app.run(host='0.0.0.0', port=5000, debug=True)

