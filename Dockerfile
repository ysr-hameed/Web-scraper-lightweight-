FROM mcr.microsoft.com/playwright:v1.51.0-noble

WORKDIR /app

ENV NODE_ENV=production
ENV HOST=0.0.0.0
ENV PORT=3000

COPY package.json ./

# Install dependencies, including TypeScript build tools.
RUN npm install --include=dev

COPY tsconfig.json ./
COPY src ./src

# Compile TypeScript.
RUN npm run build

# Use the non-root user provided by the Playwright image.
USER pwuser

EXPOSE 3000

CMD ["npm", "start"]