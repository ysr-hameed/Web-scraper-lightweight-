FROM mcr.microsoft.com/playwright:v1.64.1-noble

WORKDIR /app
ENV HOST=0.0.0.0
ENV PORT=3000

COPY package.json tsconfig.json ./
# TypeScript is a devDependency, so install devDependencies for the build stage.
RUN npm install --include=dev
COPY src ./src
RUN npm run build

# Keep runtime production-only after compilation; Chromium is provided by the base image.
ENV NODE_ENV=production
RUN npm prune --omit=dev

# Run as the non-root user supplied by the Playwright image.
USER pwuser
EXPOSE 3000
CMD ["npm", "start"]
