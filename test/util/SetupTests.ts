import { setGlobalLoggerFactory, WinstonLoggerFactory } from '@solid/community-server';

// Set the main logger
const level = process.env.LOGLEVEL ?? 'off';
const loggerFactory = new WinstonLoggerFactory(level);
setGlobalLoggerFactory(loggerFactory);
