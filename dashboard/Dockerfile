FROM node:18
WORKDIR /app
COPY ./src /app/src
COPY package.json /app/package.json
RUN npm install
CMD ["npm", "start"]
