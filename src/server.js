const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const GamificationEngine = require('./gamification-engine');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3001;
const SERVICE_NAME = 'gamification-service';

// Inicializar motor de gamificação
const gamificationEngine = new GamificationEngine();

// Middleware
app.use(helmet());
app.use(cors());
app.use(morgan('combined'));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Health check endpoint
app.get('/health', async (req, res) => {
    try {
        const metrics = await gamificationEngine.getMetrics();
        
        res.status(200).json({
            status: 'ok',
            service: SERVICE_NAME,
            timestamp: new Date().toISOString(),
            version: '1.0.0',
            environment: process.env.NODE_ENV || 'development',
            components: {
                gamificationEngine: metrics
            }
        });
    } catch (error) {
        res.status(503).json({
            status: 'degraded',
            service: SERVICE_NAME,
            timestamp: new Date().toISOString(),
            error: error.message
        });
    }
});

// Root endpoint
app.get('/', (req, res) => {
    res.json({
        service: SERVICE_NAME,
        message: `Microserviço ${SERVICE_NAME} do Sistema Fature`,
        version: '1.0.0',
        features: ['Behavior Analysis', 'Chest Optimization', 'Reward Calculation', 'AI-Powered Recommendations'],
        endpoints: {
            health: '/health',
            api: `/api/v1/${SERVICE_NAME}`,
            behaviorAnalysis: '/api/v1/gamification-service/analyze-behavior',
            chestOptimization: '/api/v1/gamification-service/optimize-chests',
            rewardCalculation: '/api/v1/gamification-service/calculate-rewards',
            testSuite: '/api/v1/gamification-service/test-suite'
        }
    });
});

// ==================== ENDPOINTS DE GAMIFICAÇÃO ====================

// Endpoint para análise comportamental
app.post('/api/v1/gamification-service/analyze-behavior', async (req, res) => {
    try {
        const userData = req.body;
        
        // Validar dados obrigatórios
        const required = ['userId'];
        const missing = required.filter(field => !userData[field]);
        
        if (missing.length > 0) {
            return res.status(400).json({
                status: 'error',
                message: `Campos obrigatórios ausentes: ${missing.join(', ')}`,
                required_fields: required
            });
        }
        
        console.log(`Analisando comportamento do usuário ${userData.userId}`);
        const analysis = await gamificationEngine.analyzeUserBehavior(userData);
        
        res.json({
            status: 'success',
            message: 'Análise comportamental executada com sucesso',
            data: analysis
        });
        
    } catch (error) {
        console.error('Erro na análise comportamental:', error);
        res.status(500).json({
            status: 'error',
            message: 'Erro interno na análise comportamental',
            error: error.message,
            timestamp: new Date().toISOString()
        });
    }
});

// Endpoint para otimização de baús
app.post('/api/v1/gamification-service/optimize-chests', async (req, res) => {
    try {
        const { userProfile, availableChests = ['silver', 'gold', 'platinum'] } = req.body;
        
        if (!userProfile || !userProfile.userId) {
            return res.status(400).json({
                status: 'error',
                message: 'userProfile com userId é obrigatório'
            });
        }
        
        console.log(`Otimizando distribuição de baús para usuário ${userProfile.userId}`);
        const optimization = await gamificationEngine.optimizeChestDistribution(userProfile, availableChests);
        
        res.json({
            status: 'success',
            message: 'Otimização de baús executada com sucesso',
            data: optimization
        });
        
    } catch (error) {
        console.error('Erro na otimização de baús:', error);
        res.status(500).json({
            status: 'error',
            message: 'Erro interno na otimização de baús',
            error: error.message,
            timestamp: new Date().toISOString()
        });
    }
});

// Endpoint para cálculo de recompensas
app.post('/api/v1/gamification-service/calculate-rewards', async (req, res) => {
    try {
        const { userActivity, chestType = 'silver' } = req.body;
        
        if (!userActivity || !userActivity.userId) {
            return res.status(400).json({
                status: 'error',
                message: 'userActivity com userId é obrigatório'
            });
        }
        
        console.log(`Calculando recompensas para usuário ${userActivity.userId}, baú ${chestType}`);
        const rewards = await gamificationEngine.calculateRewards(userActivity, chestType);
        
        res.json({
            status: 'success',
            message: 'Cálculo de recompensas executado com sucesso',
            data: rewards
        });
        
    } catch (error) {
        console.error('Erro no cálculo de recompensas:', error);
        res.status(500).json({
            status: 'error',
            message: 'Erro interno no cálculo de recompensas',
            error: error.message,
            timestamp: new Date().toISOString()
        });
    }
});

// Endpoint para suite de testes completa
app.post('/api/v1/gamification-service/test-suite', async (req, res) => {
    try {
        const testScenarios = [
            {
                name: 'Usuário VIP Ativo',
                userData: {
                    userId: 'VIP_USER_001',
                    depositHistory: [
                        { date: '2025-06-01', amount: 500 },
                        { date: '2025-06-05', amount: 300 },
                        { date: '2025-06-10', amount: 800 }
                    ],
                    betHistory: [
                        { date: '2025-06-01', amount: 50, result: 'win' },
                        { date: '2025-06-02', amount: 100, result: 'loss' },
                        { date: '2025-06-03', amount: 75, result: 'win' }
                    ],
                    sessionHistory: [
                        { startTime: '2025-06-01T20:00:00Z', duration: 120 },
                        { startTime: '2025-06-02T21:00:00Z', duration: 90 }
                    ],
                    registrationDate: '2025-01-01T00:00:00Z',
                    lastActivity: '2025-06-14T22:00:00Z'
                }
            },
            {
                name: 'Usuário Casual',
                userData: {
                    userId: 'CASUAL_USER_001',
                    depositHistory: [
                        { date: '2025-06-10', amount: 50 }
                    ],
                    betHistory: [
                        { date: '2025-06-10', amount: 10, result: 'loss' },
                        { date: '2025-06-11', amount: 15, result: 'win' }
                    ],
                    sessionHistory: [
                        { startTime: '2025-06-10T19:00:00Z', duration: 30 }
                    ],
                    registrationDate: '2025-06-01T00:00:00Z',
                    lastActivity: '2025-06-11T20:00:00Z'
                }
            },
            {
                name: 'Usuário Inativo',
                userData: {
                    userId: 'INACTIVE_USER_001',
                    depositHistory: [
                        { date: '2025-05-01', amount: 100 }
                    ],
                    betHistory: [
                        { date: '2025-05-01', amount: 20, result: 'loss' }
                    ],
                    sessionHistory: [
                        { startTime: '2025-05-01T15:00:00Z', duration: 15 }
                    ],
                    registrationDate: '2025-04-01T00:00:00Z',
                    lastActivity: '2025-05-01T16:00:00Z'
                }
            }
        ];
        
        const results = [];
        
        for (const scenario of testScenarios) {
            try {
                // Análise comportamental
                const behaviorAnalysis = await gamificationEngine.analyzeUserBehavior(scenario.userData);
                
                // Otimização de baús
                const userProfile = {
                    userId: scenario.userData.userId,
                    behaviorScore: behaviorAnalysis.score,
                    riskLevel: behaviorAnalysis.riskLevel,
                    history: scenario.userData
                };
                
                const chestOptimization = await gamificationEngine.optimizeChestDistribution(
                    userProfile, 
                    ['silver', 'gold', 'platinum']
                );
                
                // Cálculo de recompensas
                const userActivity = {
                    userId: scenario.userData.userId,
                    recentActivity: 0.8,
                    loyaltyLevel: behaviorAnalysis.riskLevel === 'low' ? 'vip' : 'standard',
                    weeklyVolume: 500
                };
                
                const rewards = await gamificationEngine.calculateRewards(userActivity, 'gold');
                
                results.push({
                    scenario: scenario.name,
                    behaviorAnalysis,
                    chestOptimization,
                    rewards,
                    summary: {
                        behaviorScore: behaviorAnalysis.score,
                        riskLevel: behaviorAnalysis.riskLevel,
                        recommendedChests: chestOptimization.recommendedChests.length,
                        expectedValue: chestOptimization.expectedValue,
                        totalRewardValue: rewards.totalValue
                    }
                });
                
            } catch (error) {
                results.push({
                    scenario: scenario.name,
                    error: error.message
                });
            }
        }
        
        res.json({
            status: 'success',
            message: 'Suite de testes de gamificação executada',
            data: {
                total_scenarios: testScenarios.length,
                results: results,
                timestamp: new Date().toISOString()
            }
        });
        
    } catch (error) {
        console.error('Erro na suite de testes:', error);
        res.status(500).json({
            status: 'error',
            message: 'Erro ao executar suite de testes',
            error: error.message
        });
    }
});

// ==================== ENDPOINTS ORIGINAIS ====================

// API principal
app.get(`/api/v1/${SERVICE_NAME}`, (req, res) => {
    res.json({
        service: SERVICE_NAME,
        message: `API do ${SERVICE_NAME} funcionando`,
        timestamp: new Date().toISOString(),
        data: {
            status: 'operational',
            features: ['behavior-analysis', 'chest-optimization', 'reward-calculation', 'ai-recommendations']
        }
    });
});

// Endpoint para status do serviço
app.get(`/api/v1/${SERVICE_NAME}/status`, (req, res) => {
    res.json({
        service: SERVICE_NAME,
        status: 'running',
        uptime: process.uptime(),
        memory: process.memoryUsage(),
        timestamp: new Date().toISOString()
    });
});

// Error handling
app.use((err, req, res, next) => {
    console.error(err.stack);
    res.status(500).json({
        error: 'Internal Server Error',
        service: SERVICE_NAME,
        timestamp: new Date().toISOString()
    });
});

// 404 handler
app.use('*', (req, res) => {
    res.status(404).json({
        error: 'Not Found',
        service: SERVICE_NAME,
        path: req.originalUrl,
        timestamp: new Date().toISOString()
    });
});

// Inicializar motor de gamificação e servidor
async function startServer() {
    try {
        console.log('🎮 Inicializando motor de gamificação...');
        await gamificationEngine.initialize();
        console.log('✅ Motor de gamificação inicializado com sucesso');
        
        // Start server
        app.listen(PORT, '0.0.0.0', () => {
            console.log(`🚀 ${SERVICE_NAME} rodando na porta ${PORT}`);
            console.log(`📊 Health check: http://localhost:${PORT}/health`);
            console.log(`🔗 API: http://localhost:${PORT}/api/v1/${SERVICE_NAME}`);
            console.log(`🧠 Análise Comportamental: http://localhost:${PORT}/api/v1/gamification-service/analyze-behavior`);
            console.log(`🎁 Otimização de Baús: http://localhost:${PORT}/api/v1/gamification-service/optimize-chests`);
            console.log(`💰 Cálculo de Recompensas: http://localhost:${PORT}/api/v1/gamification-service/calculate-rewards`);
            console.log(`🧪 Suite de Testes: http://localhost:${PORT}/api/v1/gamification-service/test-suite`);
        });
        
    } catch (error) {
        console.error('❌ Erro ao inicializar servidor:', error);
        process.exit(1);
    }
}

// Graceful shutdown
process.on('SIGTERM', () => {
    console.log('📴 Recebido SIGTERM, encerrando servidor...');
    process.exit(0);
});

process.on('SIGINT', () => {
    console.log('📴 Recebido SIGINT, encerrando servidor...');
    process.exit(0);
});

// Iniciar servidor
startServer();

