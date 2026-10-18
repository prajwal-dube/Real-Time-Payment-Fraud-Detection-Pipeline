FROM node:20-alpine
WORKDIR /app
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci
COPY dashboard/ .
EXPOSE 5173
CMD ["npm","run","dev"]
