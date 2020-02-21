FROM openjdk:7
COPY . /usr/src/myapp
WORKDIR /usr/src/myapp
RUN apt-get update 
#RUN curl -sL https://deb.nodesource.com/setup_11.x | bash -
RUN chmod +x node.sh 
RUN  ./node.sh
RUN apt-get install -y nodejs
RUN apt-get install build-essential -y
RUN npm install
EXPOSE 1212
CMD ["node", "index.js"]
