const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;
const SERVICE_NAME = 'gamification-service';

// Middleware
app.use(helmet());
app.use(cors());
app.use(morgan('combined'));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Health check endpoint
app.get('/health', (req, res) => {
    res.status(200).json({
        status: 'ok',
        service: SERVICE_NAME,
        timestamp: new Date().toISOString(),
        version: '1.0.0',
        environment: process.env.NODE_ENV || 'development'
    });
});

// Root endpoint
app.get('/', (req, res) => {
    res.json({
        service: SERVICE_NAME,
        message: `Microserviço ${SERVICE_NAME} do Sistema Fature`,
        version: '1.0.0',
        endpoints: {
            health: '/health',
            api: `/api/v1/${SERVICE_NAME}`
        }
    });
});

// API principal
app.get(`/api/v1/${SERVICE_NAME}`, (req, res) => {
    res.json({
        service: SERVICE_NAME,
        message: `API do ${SERVICE_NAME} funcionando`,
        timestamp: new Date().toISOString(),
        data: {
            status: 'operational',
            features: ['health-check', 'basic-api', 'logging']
        }
    });
});

// Endpoint para teste de conectividade
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

// Start server
app.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 ${SERVICE_NAME} rodando na porta ${PORT}`);
    console.log(`📊 Health check: http://localhost:${PORT}/health`);
    console.log(`🔗 API: http://localhost:${PORT}/api/v1/${SERVICE_NAME}`);
});

// Graceful shutdown
process.on('SIGTERM', () => {
    console.log('📴 Recebido SIGTERM, encerrando servidor...');
    process.exit(0);
});

process.on('SIGINT', () => {
    console.log('📴 Recebido SIGINT, encerrando servidor...');
    process.exit(0);
});
