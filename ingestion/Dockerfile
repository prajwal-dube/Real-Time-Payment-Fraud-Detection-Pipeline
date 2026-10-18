FROM node:20-alpine
WORKDIR /app
COPY ingestion/package.json ingestion/package-lock.json ./
RUN npm ci --omit=dev
COPY ingestion/src ./src
USER node
CMD ["npm","start"]
