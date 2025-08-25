import Fastify from 'fastify';
import cors from '@fastify/cors';
import websocket from '@fastify/websocket';
import { fetch } from 'undici';
import { z } from 'zod';

const env = {
  PORT: parseInt(process.env.PORT || '3000', 10),
  PYTHON_URL: process.env.PYTHON_URL || 'http://localhost:8000',
  API_KEY_REQUIRED: process.env.API_KEY_REQUIRED === 'true',
  API_KEY: process.env.API_KEY || ''
};

const fastify = Fastify({
  logger: {
    transport: {
      target: 'pino-pretty',
      options: { colorize: true }
    }
  }
});

await fastify.register(cors, { origin: true });
await fastify.register(websocket);

const ChatRequestSchema = z.object({
  messages: z.array(z.object({ role: z.string(), content: z.string() })).min(1),
  stream: z.boolean().optional().default(true),
  metadata: z.record(z.any()).optional()
});

function checkApiKey(req, reply) {
  if (!env.API_KEY_REQUIRED) return;
  const headerKey = req.headers['x-api-key'];
  if (!headerKey || headerKey !== env.API_KEY) {
    reply.code(401).send({ error: 'Unauthorized' });
  }
}

fastify.get('/health', async () => ({ status: 'ok' }));

fastify.post('/chat', async (req, reply) => {
  checkApiKey(req, reply);
  const parse = ChatRequestSchema.safeParse(req.body);
  if (!parse.success) {
    reply.code(400).send({ error: 'Invalid body', details: parse.error.flatten() });
    return;
  }
  const body = parse.data;
  const url = `${env.PYTHON_URL}/chat`; // SSE endpoint
  const controller = new AbortController();
  req.raw.on('close', () => controller.abort());

  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ messages: body.messages, stream: body.stream, metadata: body.metadata }),
    signal: controller.signal
  });

  if (!resp.ok) {
    const text = await resp.text();
    reply.code(502).send({ error: 'Upstream error', details: text });
    return;
  }

  // Proxy SSE stream
  reply.raw.setHeader('Content-Type', 'text/event-stream');
  reply.raw.setHeader('Cache-Control', 'no-cache');
  reply.raw.setHeader('Connection', 'keep-alive');
  reply.raw.flushHeaders?.();

  for await (const chunk of resp.body) {
    reply.raw.write(chunk);
  }
  reply.raw.end();
});

// Optional WebSocket proxy -> translates to SSE under the hood
fastify.register(async function (instance) {
  instance.get('/ws', { websocket: true }, (connection /* SocketStream */, req) => {
    connection.socket.on('message', async (raw) => {
      try {
        const payload = JSON.parse(raw.toString());
        const parse = ChatRequestSchema.safeParse(payload);
        if (!parse.success) {
          connection.socket.send(JSON.stringify({ type: 'error', error: 'Invalid body' }));
          return;
        }
        const controller = new AbortController();
        const resp = await fetch(`${env.PYTHON_URL}/chat`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ...parse.data, stream: true }),
          signal: controller.signal
        });
        if (!resp.ok || !resp.body) {
          connection.socket.send(JSON.stringify({ type: 'error', error: 'Upstream error' }));
          return;
        }
        let buffer = '';
        for await (const chunk of resp.body) {
          buffer += chunk.toString();
          const lines = buffer.split('\n');
          buffer = lines.pop() || '';
          for (const line of lines) {
            if (line.startsWith('data: ')) {
              const data = line.slice(6);
              connection.socket.send(data);
            }
          }
        }
        connection.socket.send(JSON.stringify({ type: 'done' }));
      } catch (err) {
        connection.socket.send(JSON.stringify({ type: 'error', error: String(err) }));
      }
    });
  });
});

fastify.listen({ port: env.PORT, host: '0.0.0.0' }).then((addr) => {
  fastify.log.info(`Server listening on ${addr}`);
});

