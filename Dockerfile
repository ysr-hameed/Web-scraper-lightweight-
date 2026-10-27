FROM mcr.microsoft.com/playwright:v1.51.0-noble

WORKDIR /app
ENV NODE_ENV=production
ENV HOST=0.0.0.0
ENV PORT=3000

COPY package.json tsconfig.json ./
RUN npm install
COPY src ./src
RUN npm run build

# Run as the non-root user supplied by the Playwright image.
USER pwuser
EXPOSE 3000
CMD ["npm", "start"]
