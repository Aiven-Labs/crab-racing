FROM node:20-alpine
WORKDIR /app
COPY package.json .
RUN npm install
COPY server.js .
COPY game-config.json .
COPY *.html ./
EXPOSE 3000
CMD ["node", "server.js"]
