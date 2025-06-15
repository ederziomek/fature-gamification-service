const axios = require('axios');
const redis = require('redis');

class GamificationEngine {
    constructor() {
        this.configServiceUrl = process.env.CONFIG_SERVICE_URL || 'http://localhost:5000';
        this.redisUrl = process.env.REDIS_URL || 'redis://localhost:6379';
        this.redisClient = null;
        this.localCache = new Map();
        this.cacheTtl = 300000; // 5 minutos
        
        // Algoritmos de análise comportamental
        this.behaviorAnalyzer = new BehaviorAnalyzer();
        this.chestOptimizer = new ChestOptimizer();
        this.rewardCalculator = new RewardCalculator();
    }
    
    async initialize() {
        try {
            // Conectar Redis
            this.redisClient = redis.createClient({ url: this.redisUrl });
            this.redisClient.on('error', (err) => console.log('Redis Client Error', err));
            await this.redisClient.connect();
            
            console.log('✓ Gamification Engine inicializado com sucesso');
            return true;
        } catch (error) {
            console.error('Erro ao inicializar Gamification Engine:', error);
            return false;
        }
    }
    
    async getConfig(key) {
        try {
            // Tentar cache local primeiro
            const cached = this.localCache.get(key);
            if (cached && Date.now() - cached.timestamp < this.cacheTtl) {
                return cached.value;
            }
            
            // Tentar Redis
            if (this.redisClient) {
                const redisValue = await this.redisClient.get(`config_cache:${key}`);
                if (redisValue) {
                    const value = JSON.parse(redisValue);
                    this.localCache.set(key, { value, timestamp: Date.now() });
                    return value;
                }
            }
            
            // Buscar do config-service
            const response = await axios.get(`${this.configServiceUrl}/api/v1/configurations/${key}`, {
                timeout: 5000
            });
            
            if (response.data.success) {
                const config = response.data.data;
                const value = config.get_typed_value ? config.get_typed_value() : config.value;
                
                // Cachear localmente
                this.localCache.set(key, { value, timestamp: Date.now() });
                
                return value;
            }
            
            return null;
        } catch (error) {
            console.error(`Erro ao buscar configuração ${key}:`, error.message);
            return null;
        }
    }
    
    async analyzeUserBehavior(userData) {
        try {
            const analysis = await this.behaviorAnalyzer.analyze(userData);
            
            return {
                userId: userData.userId,
                behaviorScore: analysis.score,
                riskLevel: analysis.riskLevel,
                patterns: analysis.patterns,
                recommendations: analysis.recommendations,
                timestamp: new Date().toISOString()
            };
        } catch (error) {
            console.error('Erro na análise comportamental:', error);
            throw error;
        }
    }
    
    async optimizeChestDistribution(userProfile, availableChests) {
        try {
            const optimization = await this.chestOptimizer.optimize(userProfile, availableChests);
            
            return {
                userId: userProfile.userId,
                recommendedChests: optimization.chests,
                expectedValue: optimization.expectedValue,
                confidence: optimization.confidence,
                reasoning: optimization.reasoning,
                timestamp: new Date().toISOString()
            };
        } catch (error) {
            console.error('Erro na otimização de baús:', error);
            throw error;
        }
    }
    
    async calculateRewards(userActivity, chestType) {
        try {
            const rewards = await this.rewardCalculator.calculate(userActivity, chestType);
            
            return {
                userId: userActivity.userId,
                chestType: chestType,
                rewards: rewards.items,
                totalValue: rewards.totalValue,
                bonusMultiplier: rewards.bonusMultiplier,
                timestamp: new Date().toISOString()
            };
        } catch (error) {
            console.error('Erro no cálculo de recompensas:', error);
            throw error;
        }
    }
    
    async getMetrics() {
        try {
            const metrics = {
                cache_hits: this.localCache.size,
                redis_connected: this.redisClient ? await this.redisClient.ping() === 'PONG' : false,
                behavior_analyzer_status: 'active',
                chest_optimizer_status: 'active',
                reward_calculator_status: 'active'
            };
            
            return metrics;
        } catch (error) {
            console.error('Erro ao obter métricas:', error);
            return { error: error.message };
        }
    }
}

class BehaviorAnalyzer {
    async analyze(userData) {
        const {
            userId,
            depositHistory = [],
            betHistory = [],
            sessionHistory = [],
            registrationDate,
            lastActivity
        } = userData;
        
        // Análise de padrões de depósito
        const depositAnalysis = this.analyzeDeposits(depositHistory);
        
        // Análise de padrões de apostas
        const bettingAnalysis = this.analyzeBetting(betHistory);
        
        // Análise de sessões
        const sessionAnalysis = this.analyzeSessions(sessionHistory);
        
        // Análise temporal
        const temporalAnalysis = this.analyzeTemporalPatterns(userData);
        
        // Calcular score comportamental
        const behaviorScore = this.calculateBehaviorScore({
            depositAnalysis,
            bettingAnalysis,
            sessionAnalysis,
            temporalAnalysis
        });
        
        // Determinar nível de risco
        const riskLevel = this.determineRiskLevel(behaviorScore);
        
        // Identificar padrões
        const patterns = this.identifyPatterns({
            depositAnalysis,
            bettingAnalysis,
            sessionAnalysis,
            temporalAnalysis
        });
        
        // Gerar recomendações
        const recommendations = this.generateRecommendations(behaviorScore, riskLevel, patterns);
        
        return {
            score: behaviorScore,
            riskLevel,
            patterns,
            recommendations,
            details: {
                depositAnalysis,
                bettingAnalysis,
                sessionAnalysis,
                temporalAnalysis
            }
        };
    }
    
    analyzeDeposits(depositHistory) {
        if (!depositHistory.length) {
            return { frequency: 0, averageAmount: 0, trend: 'none', volatility: 0 };
        }
        
        const amounts = depositHistory.map(d => d.amount);
        const averageAmount = amounts.reduce((a, b) => a + b, 0) / amounts.length;
        
        // Calcular frequência (depósitos por dia)
        const firstDeposit = new Date(depositHistory[0].date);
        const lastDeposit = new Date(depositHistory[depositHistory.length - 1].date);
        const daysDiff = Math.max(1, (lastDeposit - firstDeposit) / (1000 * 60 * 60 * 24));
        const frequency = depositHistory.length / daysDiff;
        
        // Calcular tendência
        const trend = this.calculateTrend(amounts);
        
        // Calcular volatilidade
        const volatility = this.calculateVolatility(amounts);
        
        return {
            frequency,
            averageAmount,
            trend,
            volatility,
            totalDeposits: depositHistory.length,
            totalAmount: amounts.reduce((a, b) => a + b, 0)
        };
    }
    
    analyzeBetting(betHistory) {
        if (!betHistory.length) {
            return { frequency: 0, averageAmount: 0, winRate: 0, riskProfile: 'unknown' };
        }
        
        const amounts = betHistory.map(b => b.amount);
        const averageAmount = amounts.reduce((a, b) => a + b, 0) / amounts.length;
        
        // Calcular taxa de vitória
        const wins = betHistory.filter(b => b.result === 'win').length;
        const winRate = wins / betHistory.length;
        
        // Calcular frequência de apostas
        const firstBet = new Date(betHistory[0].date);
        const lastBet = new Date(betHistory[betHistory.length - 1].date);
        const daysDiff = Math.max(1, (lastBet - firstBet) / (1000 * 60 * 60 * 24));
        const frequency = betHistory.length / daysDiff;
        
        // Determinar perfil de risco
        const riskProfile = this.determineRiskProfile(amounts, betHistory);
        
        return {
            frequency,
            averageAmount,
            winRate,
            riskProfile,
            totalBets: betHistory.length,
            totalAmount: amounts.reduce((a, b) => a + b, 0)
        };
    }
    
    analyzeSessions(sessionHistory) {
        if (!sessionHistory.length) {
            return { averageDuration: 0, frequency: 0, peakHours: [] };
        }
        
        // Calcular duração média das sessões
        const durations = sessionHistory.map(s => s.duration || 0);
        const averageDuration = durations.reduce((a, b) => a + b, 0) / durations.length;
        
        // Calcular frequência de sessões
        const firstSession = new Date(sessionHistory[0].startTime);
        const lastSession = new Date(sessionHistory[sessionHistory.length - 1].startTime);
        const daysDiff = Math.max(1, (lastSession - firstSession) / (1000 * 60 * 60 * 24));
        const frequency = sessionHistory.length / daysDiff;
        
        // Identificar horários de pico
        const peakHours = this.identifyPeakHours(sessionHistory);
        
        return {
            averageDuration,
            frequency,
            peakHours,
            totalSessions: sessionHistory.length
        };
    }
    
    analyzeTemporalPatterns(userData) {
        const registrationDate = new Date(userData.registrationDate);
        const lastActivity = new Date(userData.lastActivity);
        const accountAge = (Date.now() - registrationDate.getTime()) / (1000 * 60 * 60 * 24);
        const daysSinceLastActivity = (Date.now() - lastActivity.getTime()) / (1000 * 60 * 60 * 24);
        
        return {
            accountAge,
            daysSinceLastActivity,
            activityLevel: this.calculateActivityLevel(accountAge, daysSinceLastActivity)
        };
    }
    
    calculateBehaviorScore(analyses) {
        let score = 50; // Score base
        
        // Ajustar baseado em depósitos
        if (analyses.depositAnalysis.frequency > 0.1) score += 10;
        if (analyses.depositAnalysis.averageAmount > 100) score += 10;
        if (analyses.depositAnalysis.volatility < 0.5) score += 5;
        
        // Ajustar baseado em apostas
        if (analyses.bettingAnalysis.winRate > 0.4) score += 10;
        if (analyses.bettingAnalysis.frequency > 1) score += 5;
        if (analyses.bettingAnalysis.riskProfile === 'conservative') score += 10;
        
        // Ajustar baseado em sessões
        if (analyses.sessionAnalysis.frequency > 0.5) score += 5;
        if (analyses.sessionAnalysis.averageDuration > 30) score += 5;
        
        // Ajustar baseado em padrões temporais
        if (analyses.temporalAnalysis.activityLevel === 'high') score += 10;
        if (analyses.temporalAnalysis.daysSinceLastActivity < 7) score += 5;
        
        return Math.min(100, Math.max(0, score));
    }
    
    determineRiskLevel(score) {
        if (score >= 80) return 'low';
        if (score >= 60) return 'medium';
        if (score >= 40) return 'high';
        return 'very_high';
    }
    
    identifyPatterns(analyses) {
        const patterns = [];
        
        // Padrões de depósito
        if (analyses.depositAnalysis.frequency > 1) {
            patterns.push('frequent_depositor');
        }
        if (analyses.depositAnalysis.volatility > 1) {
            patterns.push('volatile_deposits');
        }
        
        // Padrões de apostas
        if (analyses.bettingAnalysis.riskProfile === 'aggressive') {
            patterns.push('high_risk_bettor');
        }
        if (analyses.bettingAnalysis.winRate < 0.3) {
            patterns.push('frequent_loser');
        }
        
        // Padrões temporais
        if (analyses.temporalAnalysis.daysSinceLastActivity > 30) {
            patterns.push('inactive_user');
        }
        
        return patterns;
    }
    
    generateRecommendations(score, riskLevel, patterns) {
        const recommendations = [];
        
        if (riskLevel === 'low') {
            recommendations.push('Oferecer baús premium');
            recommendations.push('Programa VIP');
        } else if (riskLevel === 'medium') {
            recommendations.push('Baús padrão com bônus');
            recommendations.push('Promoções moderadas');
        } else {
            recommendations.push('Baús básicos');
            recommendations.push('Monitoramento aumentado');
        }
        
        if (patterns.includes('inactive_user')) {
            recommendations.push('Campanha de reativação');
        }
        
        if (patterns.includes('frequent_depositor')) {
            recommendations.push('Programa de fidelidade');
        }
        
        return recommendations;
    }
    
    // Métodos auxiliares
    calculateTrend(values) {
        if (values.length < 2) return 'stable';
        
        const firstHalf = values.slice(0, Math.floor(values.length / 2));
        const secondHalf = values.slice(Math.floor(values.length / 2));
        
        const firstAvg = firstHalf.reduce((a, b) => a + b, 0) / firstHalf.length;
        const secondAvg = secondHalf.reduce((a, b) => a + b, 0) / secondHalf.length;
        
        if (secondAvg > firstAvg * 1.1) return 'increasing';
        if (secondAvg < firstAvg * 0.9) return 'decreasing';
        return 'stable';
    }
    
    calculateVolatility(values) {
        if (values.length < 2) return 0;
        
        const mean = values.reduce((a, b) => a + b, 0) / values.length;
        const variance = values.reduce((sum, val) => sum + Math.pow(val - mean, 2), 0) / values.length;
        return Math.sqrt(variance) / mean;
    }
    
    determineRiskProfile(amounts, betHistory) {
        const avgAmount = amounts.reduce((a, b) => a + b, 0) / amounts.length;
        const maxAmount = Math.max(...amounts);
        
        if (maxAmount > avgAmount * 5) return 'aggressive';
        if (avgAmount < 10) return 'conservative';
        return 'moderate';
    }
    
    identifyPeakHours(sessionHistory) {
        const hourCounts = {};
        
        sessionHistory.forEach(session => {
            const hour = new Date(session.startTime).getHours();
            hourCounts[hour] = (hourCounts[hour] || 0) + 1;
        });
        
        const sortedHours = Object.entries(hourCounts)
            .sort(([,a], [,b]) => b - a)
            .slice(0, 3)
            .map(([hour]) => parseInt(hour));
        
        return sortedHours;
    }
    
    calculateActivityLevel(accountAge, daysSinceLastActivity) {
        if (daysSinceLastActivity < 1) return 'very_high';
        if (daysSinceLastActivity < 7) return 'high';
        if (daysSinceLastActivity < 30) return 'medium';
        return 'low';
    }
}

class ChestOptimizer {
    async optimize(userProfile, availableChests) {
        const {
            userId,
            behaviorScore,
            riskLevel,
            preferences = {},
            history = []
        } = userProfile;
        
        // Obter configurações de gamificação
        const configs = await this.getGamificationConfigs();
        
        // Calcular potencial de cada tipo de baú
        const chestPotentials = await this.calculateChestPotentials(userProfile, availableChests, configs);
        
        // Aplicar algoritmo de otimização
        const optimizedDistribution = this.optimizeDistribution(chestPotentials, configs);
        
        // Calcular valor esperado
        const expectedValue = this.calculateExpectedValue(optimizedDistribution);
        
        // Calcular confiança da recomendação
        const confidence = this.calculateConfidence(userProfile, optimizedDistribution);
        
        return {
            chests: optimizedDistribution,
            expectedValue,
            confidence,
            reasoning: this.generateReasoning(userProfile, optimizedDistribution)
        };
    }
    
    async getGamificationConfigs() {
        // Simulação das configurações - em produção viria do config-service
        return {
            silver: {
                successRate: 0.7,
                minValue: 10.0,
                maxValue: 50.0
            },
            gold: {
                successRate: 0.5,
                minValue: 25.0,
                maxValue: 100.0
            },
            platinum: {
                successRate: 0.3,
                minValue: 50.0,
                maxValue: 200.0
            }
        };
    }
    
    async calculateChestPotentials(userProfile, availableChests, configs) {
        const potentials = {};
        
        for (const chestType of availableChests) {
            const config = configs[chestType];
            if (!config) continue;
            
            // Calcular potencial baseado no perfil do usuário
            let potential = config.successRate;
            
            // Ajustar baseado no comportamento
            if (userProfile.behaviorScore > 80) {
                potential *= 1.2; // Usuários de alto score têm maior chance
            } else if (userProfile.behaviorScore < 40) {
                potential *= 0.8; // Usuários de baixo score têm menor chance
            }
            
            // Ajustar baseado no nível de risco
            if (userProfile.riskLevel === 'low') {
                potential *= 1.1;
            } else if (userProfile.riskLevel === 'very_high') {
                potential *= 0.7;
            }
            
            potentials[chestType] = {
                potential: Math.min(1.0, potential),
                expectedValue: (config.minValue + config.maxValue) / 2 * potential,
                config
            };
        }
        
        return potentials;
    }
    
    optimizeDistribution(chestPotentials, configs) {
        const distribution = [];
        
        // Ordenar por valor esperado
        const sortedChests = Object.entries(chestPotentials)
            .sort(([,a], [,b]) => b.expectedValue - a.expectedValue);
        
        // Distribuir baús baseado na otimização
        for (const [chestType, data] of sortedChests) {
            if (data.potential > 0.3) { // Só recomendar se potencial > 30%
                distribution.push({
                    type: chestType,
                    quantity: this.calculateOptimalQuantity(data),
                    potential: data.potential,
                    expectedValue: data.expectedValue
                });
            }
        }
        
        return distribution;
    }
    
    calculateOptimalQuantity(chestData) {
        // Algoritmo simples - em produção seria mais sofisticado
        if (chestData.potential > 0.8) return 3;
        if (chestData.potential > 0.6) return 2;
        return 1;
    }
    
    calculateExpectedValue(distribution) {
        return distribution.reduce((total, chest) => {
            return total + (chest.expectedValue * chest.quantity);
        }, 0);
    }
    
    calculateConfidence(userProfile, distribution) {
        // Calcular confiança baseada na quantidade de dados do usuário
        let confidence = 0.5; // Base
        
        if (userProfile.history && userProfile.history.length > 10) {
            confidence += 0.2;
        }
        
        if (userProfile.behaviorScore > 60) {
            confidence += 0.2;
        }
        
        if (distribution.length > 0) {
            confidence += 0.1;
        }
        
        return Math.min(1.0, confidence);
    }
    
    generateReasoning(userProfile, distribution) {
        const reasons = [];
        
        if (userProfile.behaviorScore > 80) {
            reasons.push('Usuário com alto score comportamental');
        }
        
        if (userProfile.riskLevel === 'low') {
            reasons.push('Perfil de baixo risco identificado');
        }
        
        if (distribution.length > 1) {
            reasons.push('Distribuição diversificada recomendada');
        }
        
        return reasons;
    }
}

class RewardCalculator {
    async calculate(userActivity, chestType) {
        const baseRewards = await this.getBaseRewards(chestType);
        const bonusMultiplier = this.calculateBonusMultiplier(userActivity);
        
        const finalRewards = baseRewards.map(reward => ({
            ...reward,
            value: reward.value * bonusMultiplier
        }));
        
        const totalValue = finalRewards.reduce((sum, reward) => sum + reward.value, 0);
        
        return {
            items: finalRewards,
            totalValue,
            bonusMultiplier
        };
    }
    
    async getBaseRewards(chestType) {
        // Simulação de recompensas base
        const rewardTables = {
            silver: [
                { type: 'coins', value: 10, probability: 0.8 },
                { type: 'bonus', value: 5, probability: 0.6 },
                { type: 'free_spins', value: 3, probability: 0.4 }
            ],
            gold: [
                { type: 'coins', value: 25, probability: 0.7 },
                { type: 'bonus', value: 15, probability: 0.5 },
                { type: 'free_spins', value: 10, probability: 0.3 },
                { type: 'cashback', value: 20, probability: 0.2 }
            ],
            platinum: [
                { type: 'coins', value: 50, probability: 0.6 },
                { type: 'bonus', value: 30, probability: 0.4 },
                { type: 'free_spins', value: 20, probability: 0.3 },
                { type: 'cashback', value: 50, probability: 0.2 },
                { type: 'vip_points', value: 100, probability: 0.1 }
            ]
        };
        
        const table = rewardTables[chestType] || rewardTables.silver;
        
        return table.filter(reward => Math.random() < reward.probability);
    }
    
    calculateBonusMultiplier(userActivity) {
        let multiplier = 1.0;
        
        // Bônus por atividade recente
        if (userActivity.recentActivity > 0.8) {
            multiplier += 0.2;
        }
        
        // Bônus por fidelidade
        if (userActivity.loyaltyLevel === 'vip') {
            multiplier += 0.3;
        } else if (userActivity.loyaltyLevel === 'premium') {
            multiplier += 0.15;
        }
        
        // Bônus por volume de apostas
        if (userActivity.weeklyVolume > 1000) {
            multiplier += 0.1;
        }
        
        return multiplier;
    }
}

module.exports = GamificationEngine;

