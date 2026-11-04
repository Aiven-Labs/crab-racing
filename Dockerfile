FROM node:20-alpine
WORKDIR /app
COPY package.json .
RUN npm install
COPY server.js events.js ./
# Ship the example as the default track config. Mount your own game-config.json
# over it (or COPY it in a downstream image) to customise — the code falls back
# to built-in defaults if neither is present.
COPY game-config.example.json ./game-config.json
COPY *.html ./
# Static assets served straight off disk by server.js's serveStatic() — the
# Aiven logo + design-system fonts (@font-face rules in game.html) live here.
COPY assets ./assets
COPY _ds ./_ds
EXPOSE 3000
CMD ["node", "server.js"]
