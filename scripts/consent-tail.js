#!/usr/bin/env node
/**
 * Tail the marketing-consent Kafka topic — for checking what /event/<slug>
 * is publishing. Uses the same env as server.js (run via `mise run consent:tail`
 * so fnox supplies the Aiven Kafka certs).
 *
 *   node scripts/consent-tail.js            # new records only
 *   node scripts/consent-tail.js --from-beginning
 */
require('dotenv').config();
const fs = require('fs');
const crypto = require('crypto');
const { Kafka } = require('kafkajs');

const TOPIC = process.env.MARKETING_TOPIC || 'marketing-consent';
const readPem = (v) => !v ? undefined
  : v.includes('-----BEGIN') ? v.replace(/\\n/g, '\n') : fs.readFileSync(v, 'utf8');

const cfg = { clientId: 'crab-racing-consent-tail', brokers: [process.env.KAFKA_BROKER || 'localhost:9092'], logLevel: 1 };
if ((process.env.KAFKA_SECURITY_PROTOCOL || '').toUpperCase() === 'SSL') {
  cfg.ssl = {
    ca:   [readPem(process.env.KAFKA_CA_CERT)],
    key:  readPem(process.env.KAFKA_ACCESS_KEY),
    cert: readPem(process.env.KAFKA_ACCESS_CERT),
  };
}

(async () => {
  // Throwaway group so tailing never moves a real consumer's offsets.
  const consumer = new Kafka(cfg).consumer({ groupId: `consent-tail-${crypto.randomUUID()}` });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: process.argv.includes('--from-beginning') });
  console.error(`tailing ${TOPIC} on ${cfg.brokers[0]} — Ctrl-C to stop`);
  await consumer.run({ eachMessage: async ({ message }) => console.log(message.value.toString()) });
  const stop = () => consumer.disconnect().finally(() => process.exit(0));
  process.on('SIGINT', stop); process.on('SIGTERM', stop);
})().catch(err => { console.error('error:', err.message); process.exit(1); });
