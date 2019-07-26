FROM nginx

RUN rm /etc/nginx/conf.d/default.conf

COPY ./neo4j.conf /etc/nginx/conf.d/neo4j.conf

COPY ./ssl.conf /etc/ssl.conf
ENTRYPOINT nginx -g "daemon off;"

