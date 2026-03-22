FROM node:22-slim
WORKDIR /app
COPY package*.json ./
RUN npm install --omit=dev --no-package-lock
COPY src/ ./src/
RUN mkdir -p /data
VOLUME ["/data"]
EXPOSE 4600
ENV DB_PATH=/data/axon.db
CMD ["node", "--experimental-strip-types", "src/server.ts"]
